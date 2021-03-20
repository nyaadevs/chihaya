package whitelist

import (
	"context"
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

func NewHook(cfg nya.Config) (middleware.Hook, error) {
	log.Debugf("creating new whitelist middleware with config: %#v", cfg)
	h := &hook{
		closing: make(chan struct{}),
	}

	nyaCtx, err := nya.NewContext(cfg)
	if err != nil {
		return nil, err
	}
	nya.Ctx = nyaCtx

	go func() {
		for {
			select {
			case <-h.closing:
				return
			case <-time.After(time.Minute):
				log.Debug("performing torrent fetch")
				// TODO
			}
		}
	}()

	return h, nil
}

func (h *hook) Stop() <-chan error {
	log.Debug("attempting to shutdown whitelist middleware")
	select {
	case <-h.closing:
		return stop.AlreadyStopped
	default:
	}
	c := make(chan error)
	go func() {
		close(h.closing)
		close(c)
	}()
	return c
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) (context.Context, error) {
	_, err := nya.Ctx.LookupTorrent(req.InfoHash)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}

func (h *hook) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest, resp *bittorrent.ScrapeResponse) (context.Context, error) {
	return ctx, nil
}

func (h *hook) HandleApi(ctx context.Context, req *bittorrent.ApiRequest, resp *bittorrent.ApiResponse) (context.Context, error) {
	err := nya.Ctx.HandleApi(req, resp)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}
