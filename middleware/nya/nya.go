package nya

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/chihaya/chihaya/bittorrent"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrUnregisteredTorrent = bittorrent.ClientError("torrent not found")
)

type Config struct {
	DatabaseURI        string        `yaml:"database_uri"`
	TorrentsTable      string        `yaml:"torrents_table"`
	StatsTable         string        `yaml:"stats_table"`
	StatsQueryTries    int           `yaml:"stats_query_tries"`
	StatsQueryInterval time.Duration `yaml:"stats_query_interval"`
}

type Torrent struct {
	ID        uint64
	Completed uint32 // Downloads completed since the last flush
	Seeds     uint32 // Current number of seeds
	Leechers  uint32 // Current number of leechers
}

type Context struct {
	Cfg Config
	db  *sql.DB

	torrents map[bittorrent.InfoHash]Torrent
	dirty    map[bittorrent.InfoHash]struct{}
	lock     sync.RWMutex
}

var Ctx *Context

func NewContext(cfg Config) (*Context, error) {
	db, err := sql.Open("mysql", cfg.DatabaseURI)
	if err != nil {
		return nil, err
	}

	if cfg.StatsQueryTries < 1 {
		cfg.StatsQueryTries = 1
	}

	return &Context{
		Cfg:      cfg,
		db:       db,
		torrents: map[bittorrent.InfoHash]Torrent{},
		dirty:    map[bittorrent.InfoHash]struct{}{},
	}, nil
}

func (ctx *Context) LookupTorrent(info bittorrent.InfoHash) (Torrent, error) {
	ctx.lock.RLock()
	torrent, exists := ctx.torrents[info]
	ctx.lock.RUnlock()
	if exists {
		return torrent, nil
	}

	torrent = Torrent{}
	hexHash := hex.EncodeToString(info[:])

	row := ctx.db.QueryRow("SELECT id FROM " + ctx.Cfg.TorrentsTable + " WHERE info_hash = x'" + hexHash + "'")
	err := row.Scan(&torrent.ID)

	switch {
	case err == sql.ErrNoRows:
		return torrent, ErrUnregisteredTorrent
	case err != nil:
		return torrent, err
	default:
		ctx.lock.Lock()
		ctx.torrents[info] = torrent
		ctx.lock.Unlock()
		return torrent, nil
	}
}

func (ctx *Context) RecordStats(info bittorrent.InfoHash, seeds uint32, leechers uint32, completed bool) {
	ctx.lock.Lock()
	torrent, exists := ctx.torrents[info]
	if exists {
		torrent.Seeds = seeds
		torrent.Leechers = leechers
		if completed {
			torrent.Completed++
		}
		ctx.torrents[info] = torrent
		ctx.dirty[info] = struct{}{}
	}
	ctx.lock.Unlock()
}

func (ctx *Context) FlushStats() {
	var query bytes.Buffer

	count := 0
	ctx.lock.Lock()

	if len(ctx.dirty) > 0 {
		query.WriteString("INSERT INTO ")
		query.WriteString(ctx.Cfg.StatsTable)
		query.WriteString(" (torrent_id, seed_count, leech_count, download_count, last_updated) VALUES ")

		for info, _ := range ctx.dirty {
			torrent, exists := ctx.torrents[info]
			if exists {
				count++

				if count > 1 {
					query.WriteByte(',')
				}

				query.WriteByte('(')
				query.WriteString(strconv.FormatUint(torrent.ID, 10))
				query.WriteByte(',')
				query.WriteString(strconv.FormatUint(uint64(torrent.Seeds), 10))
				query.WriteByte(',')
				query.WriteString(strconv.FormatUint(uint64(torrent.Leechers), 10))
				query.WriteByte(',')
				query.WriteString(strconv.FormatUint(uint64(torrent.Completed), 10))
				query.WriteString(",NOW())")

				if torrent.Completed > 0 {
					torrent.Completed = 0 // Reset for the next flush
					ctx.torrents[info] = torrent
				}
			}
		}

		query.WriteString(" ON DUPLICATE KEY UPDATE " +
			"seed_count = VALUES(seed_count), " +
			"leech_count = VALUES(leech_count), " +
			"download_count = download_count + VALUES(download_count), " +
			"last_updated = VALUES(last_updated);")

		ctx.dirty = map[bittorrent.InfoHash]struct{}{}
	}

	ctx.lock.Unlock()

	if count > 0 {
		str := query.String()
		for try := 1; try <= ctx.Cfg.StatsQueryTries; try++ {
			start := time.Now()
			_, err := ctx.db.Exec(str)
			if err == nil {
				log.Infof("saved %d stat rows in %s", count, time.Now().Sub(start))
				break
			}
			log.Errorf("error saving stats (try %d/%d): %s", try, ctx.Cfg.StatsQueryTries, err)
			time.Sleep(time.Second * time.Duration(try))
		}
	}
}
