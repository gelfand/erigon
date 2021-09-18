package filters

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/puzpuzpuz/xsync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
)

type Filters struct {
	wg sync.WaitGroup

	pendingBlock atomic.UnsafePointer
	// pendingBlock *types.Block

	headsSubs        xsync.Map
	pendingLogsSubs  xsync.Map
	pendingBlockSubs xsync.Map
	pendingTxsSubs   xsync.Map
}

func New(ctx context.Context, ethBackend services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient) *Filters {
	log.Info("rpc filters: subscribing to Erigon events")
	ff := &Filters{
		wg:               sync.WaitGroup{},
		pendingBlock:     atomic.UnsafePointer{},
		headsSubs:        *xsync.NewMap(),
		pendingLogsSubs:  *xsync.NewMap(),
		pendingBlockSubs: *xsync.NewMap(),
		pendingTxsSubs:   *xsync.NewMap(),
	}

	go func() {
		if ethBackend == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := ethBackend.Subscribe(ctx, ff.OnNewEvent); err != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
					time.Sleep(time.Second)
					continue
				}
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					time.Sleep(time.Second)
					continue
				}

				log.Warn("rpc filters: error subscribing to events", "err", err)
				time.Sleep(time.Second)
			}
		}
	}()

	if txPool != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := ff.subscribeToPendingTransactions(ctx, txPool); err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
						time.Sleep(time.Second)
						continue
					}
					if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
						time.Sleep(time.Second)
						continue
					}
					log.Warn("rpc filters: error subscribing to pending transactions", "err", err)
					time.Sleep(time.Second)
				}
			}
		}()
		if !reflect.ValueOf(mining).IsNil() { // https://groups.google.com/g/golang-nuts/c/wnH302gBa4I
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err := ff.subscribeToPendingBlocks(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							return
						default:
						}
						if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
							time.Sleep(time.Second)
							continue
						}
						if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
							time.Sleep(time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending blocks", "err", err)
						time.Sleep(time.Second)
					}
				}
			}()
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err := ff.subscribeToPendingLogs(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							return
						default:
						}
						if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
							time.Sleep(time.Second)
							continue
						}
						if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
							time.Sleep(time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending logs", "err", err)
						time.Sleep(time.Second)
					}
				}
			}()
		}
	}

	return ff
}

func (ff *Filters) LastPendingBlock() *types.Block {
	return (*types.Block)(ff.pendingBlock.Load())
}

func (ff *Filters) subscribeToPendingTransactions(ctx context.Context, txPool txpool.TxpoolClient) error {
	subscription, err := txPool.OnAdd(ctx, &txpool.OnAddRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.OnNewTx(event)
	}
	return nil
}

func (ff *Filters) subscribeToPendingBlocks(ctx context.Context, mining txpool.MiningClient) error {
	subscription, err := mining.OnPendingBlock(ctx, &txpool.OnPendingBlockRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingBlock(event)
	}
	return nil
}

func (ff *Filters) HandlePendingBlock(reply *txpool.OnPendingBlockReply) {
	b := &types.Block{}
	if err := rlp.Decode(bytes.NewReader(reply.RplBlock), b); err != nil {
		log.Warn("OnNewTx rpc filters, unprocessable payload", "err", err)
	}
	blockPtr := unsafe.Pointer(b)
	ff.pendingBlock.Store(blockPtr)

	ff.pendingBlockSubs.Range(func(_ string, value interface{}) bool {
		ff.wg.Add(1)
		ch := value.(chan *types.Block)
		go func() {
			defer ff.wg.Done()
			ch <- b
		}()
		return true
	})
	ff.wg.Wait()

	// ff.pendingBlockSubs.Range(func(_, value interface{}) bool {
	// 	ff.wg.Add(1)
	// 	ch := value.(chan *types.Block)
	// 	go func() {
	// 		defer ff.wg.Done()
	// 		ch <- b
	// 	}()
	// 	return true
	// })
	// ff.wg.Wait()
}

func (ff *Filters) subscribeToPendingLogs(ctx context.Context, mining txpool.MiningClient) error {
	subscription, err := mining.OnPendingLogs(ctx, &txpool.OnPendingLogsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingLogs(event)
	}
	return nil
}

func (ff *Filters) HandlePendingLogs(reply *txpool.OnPendingLogsReply) {
	l := []*types.Log{}
	if err := rlp.Decode(bytes.NewReader(reply.RplLogs), &l); err != nil {
		log.Warn("OnNewTx rpc filters, unprocessable payload", "err", err)
	}

	ff.pendingLogsSubs.Range(func(_ string, value interface{}) bool {
		ff.wg.Add(1)
		ch := value.(chan types.Logs)
		go func() {
			defer ff.wg.Done()
			ch <- l
		}()
		return true
	})

	// ff.pendingLogsSubs.Range(func(_, value interface{}) bool {
	// 	ff.wg.Add(1)
	// 	ch := value.(chan types.Logs)
	// 	go func() {
	// 		defer ff.wg.Done()
	// 		ch <- l
	// 	}()
	// 	return true
	// })
	// ff.wg.Wait()
}

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) HeadsSubID {
	id := HeadsSubID(generateSubscriptionID())
	ff.headsSubs.Store(string(id), out)
	return id
}

func (ff *Filters) UnsubscribeHeads(id HeadsSubID) {
	ff.headsSubs.Delete(string(id))
}

func (ff *Filters) SubscribePendingLogs(c chan types.Logs) PendingLogsSubID {
	id := PendingLogsSubID(generateSubscriptionID())
	ff.pendingLogsSubs.Store(string(id), c)
	return id
}

func (ff *Filters) UnsubscribePendingLogs(id PendingLogsSubID) {
	ff.pendingLogsSubs.Delete(string(id))
}

func (ff *Filters) SubscribePendingBlock(f chan *types.Block) PendingBlockSubID {
	id := PendingBlockSubID(generateSubscriptionID())
	ff.pendingBlockSubs.Store(string(id), f)
	return id
}

func (ff *Filters) UnsubscribePendingBlock(id PendingBlockSubID) {
	ff.pendingBlockSubs.Delete(string(id))
}

func (ff *Filters) SubscribePendingTxs(out chan []types.Transaction) PendingTxsSubID {
	id := PendingTxsSubID(generateSubscriptionID())
	ff.pendingTxsSubs.Store(string(id), out)
	return id
}

func (ff *Filters) UnsubscribePendingTxs(id PendingTxsSubID) {
	ff.pendingTxsSubs.Delete(string(id))
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	switch event.Type {
	case remote.Event_HEADER:
		payload := event.Data
		var header types.Header

		err := rlp.Decode(bytes.NewReader(payload), &header)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewEvent rpc filters (header), unprocessable payload", "err", err)
		} else {
			ff.headsSubs.Range(func(_ string, value interface{}) bool {
				ff.wg.Add(1)
				ch := value.(chan *types.Header)
				go func() {
					defer ff.wg.Done()
					ch <- &header
				}()
				return true
			})
			ff.wg.Wait()
		}
	// case remote.Event_PENDING_LOGS:
	//	payload := event.Data
	//	var logs types.Logs
	//	err := rlp.Decode(bytes.NewReader(payload), &logs)
	//	if err != nil {
	//		// ignoring what we can't unmarshal
	//		log.Warn("OnNewEvent rpc filters (pending logs), unprocessable payload", "err", err)
	//	} else {
	//		for _, v := range ff.pendingLogsSubs {
	//			v <- logs
	//		}
	//	}
	// case remote.Event_PENDING_BLOCK:
	//	payload := event.Data
	//	var block types.Block
	//	err := rlp.Decode(bytes.NewReader(payload), &block)
	//	if err != nil {
	//		// ignoring what we can't unmarshal
	//		log.Warn("OnNewEvent rpc filters (pending txs), unprocessable payload", "err", err)
	//	} else {
	//		for _, v := range ff.pendingBlockSubs {
	//			v <- &block
	//		}
	//	}
	default:
		log.Warn("OnNewEvent rpc filters: unsupported event type", "type", event.Type)
		return
	}
}

func (ff *Filters) OnNewTx(reply *txpool.OnAddReply) {
	txs := make([]types.Transaction, len(reply.RplTxs))
	for i, rlpTx := range reply.RplTxs {
		var err error
		s := rlp.NewStream(bytes.NewReader(rlpTx), uint64(len(rlpTx)))
		txs[i], err = types.DecodeTransaction(s)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewTx rpc filters, unprocessable payload", "err", err, "data", fmt.Sprintf("%x", rlpTx))
			break
		}
	}
	ff.pendingTxsSubs.Range(func(_ string, value interface{}) bool {
		ch := value.(chan []types.Transaction)
		ff.wg.Add(1)
		go func() {
			defer ff.wg.Done()
			ch <- txs
		}()
		return true
	})
	ff.wg.Wait()
}

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}
