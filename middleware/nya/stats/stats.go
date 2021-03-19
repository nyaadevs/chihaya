package stats

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware"
	"github.com/chihaya/chihaya/middleware/nya"
	"github.com/chihaya/chihaya/pkg/stop"
)

type hook struct {
	closing chan struct{}
}

func NewHook() (middleware.Hook, error) {
	log.Debugf("creating new stats middleware")
	h := &hook{
		closing: make(chan struct{}),
	}

	if nya.Ctx == nil {
		return nil, errors.New("must add whitelist prehook")
	}

	go func() {
		for {
			select {
			case <-h.closing:
				return
			case <-time.After(nya.Ctx.Cfg.StatsQueryInterval):
				log.Debug("performing stats flush")
				nya.Ctx.FlushStats()
			}
		}
	}()

	return h, nil
}

func (h *hook) Stop() <-chan error {
	log.Debug("attempting to shutdown stats middleware")
	select {
	case <-h.closing:
		return stop.AlreadyStopped
	default:
	}
	c := make(chan error)
	go func() {
		close(h.closing)
		log.Info("performing final stats flush")
		nya.Ctx.FlushStats()
		close(c)
	}()
	return c
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) (context.Context, error) {
	nya.Ctx.RecordStats(req.InfoHash, resp.Complete, resp.Incomplete, req.Event == bittorrent.Completed)
	return ctx, nil
}

func (h *hook) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest, resp *bittorrent.ScrapeResponse) (context.Context, error) {
	return ctx, nil
}

func (h *hook) HandleApi(ctx context.Context, req *bittorrent.ApiRequest, resp *bittorrent.ApiResponse) (context.Context, error) {
	return ctx, nil
}
