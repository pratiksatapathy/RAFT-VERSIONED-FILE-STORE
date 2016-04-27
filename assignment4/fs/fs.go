package fs

import (
	_ "fmt"
	"sync"
	"time"

	"github.com/pratiksatapathy/cs733/assignment4/raftnode"
	"encoding/json"
)

type FileInfo struct {
	filename   string
	contents   []byte
	version    int
	absexptime time.Time
	timer      *time.Timer
}

type FS struct {
	sync.RWMutex
	dir      map[string]*FileInfo
	gversion int // global version
	lastindex int64
}
type Mapper struct {
	MutX_ClientMap *sync.RWMutex
	ChanDir        map[int]chan interface{}
}

//remove the sm instance for tis
//type COMMIT_TO_CLIENT struct {
//	Index    int64
//	Data     []byte
//	Err_code int64
//	//handler  int
//}
type MsgWithId struct {
	Handler int
	Mesg    Msg
}


func (fi *FileInfo) cancelTimer() {
	if fi.timer != nil {
		fi.timer.Stop()
		fi.timer = nil
	}
}
//var fs FS;
func StartFileStore(rn *raftnode.RaftNode, clientmp *Mapper) {
	fs := &FS{dir: make(map[string]*FileInfo, 1000)}
	var msgwithId *MsgWithId
	var cmt_t_client raftnode.COMMIT_TO_CLIENT

	for {

		cmt_t_client = <-rn.CommitChan

		err := json.Unmarshal(cmt_t_client.Data, &msgwithId)

		if err != nil {
			panic(err)
		}


		if fs.lastindex < cmt_t_client.Index {
			//fmt.Println("L:",rn.LeaderId(),":cmt NOdeid:",rn.Id(),"index: ",cmt_t_client.Index,"Data:",msgwithId.Mesg.Filename,"version:",fs.dir)


			response := fs.ProcessMsg(&(msgwithId.Mesg))
			fs.lastindex = cmt_t_client.Index

			//fmt.Println("lockunlock")
			clientmp.MutX_ClientMap.Lock()

			if chn, ok := clientmp.ChanDir[msgwithId.Handler]; ok {
				//do something here
				chn <- response
			}

			clientmp.MutX_ClientMap.Unlock()
		}

	}

}
func (fs *FS) ProcessMsg(msg *Msg) *Msg {
	switch msg.Kind {
	case 'r':
		return fs.processRead(msg)
	case 'w':
		return fs.processWrite(msg)
	case 'c':
		return fs.processCas(msg)
	case 'd':
		return fs.processDelete(msg)
	}

	// Default: Internal error. Shouldn't come here since
	// the msg should have been validated earlier.
	return &Msg{Kind: 'I'}
}

func (fs *FS) processRead(msg *Msg) *Msg {
	fs.RLock()
	defer fs.RUnlock()
	if fi := fs.dir[msg.Filename]; fi != nil {
		remainingTime := 0
		if fi.timer != nil {
			remainingTime := int(fi.absexptime.Sub(time.Now()))
			if remainingTime < 0 {
				remainingTime = 0
			}
		}
		return &Msg{
			Kind:     'C',
			Filename: fi.filename,
			Contents: fi.contents,
			Numbytes: len(fi.contents),
			Exptime:  remainingTime,
			Version:  fi.version,
		}
	} else {
		return &Msg{Kind: 'F'} // file not found
	}
}

func (fs *FS) internalWrite(msg *Msg) *Msg {
	fi := fs.dir[msg.Filename]
	if fi != nil {
		fi.cancelTimer()
	} else {
		fi = &FileInfo{}
	}

	fs.gversion += 1
	fi.filename = msg.Filename
	fi.contents = msg.Contents
	fi.version = fs.gversion

	var absexptime time.Time
	if msg.Exptime > 0 {
		dur := time.Duration(msg.Exptime) * time.Second
		absexptime = time.Now().Add(dur)
		timerFunc := func(name string, ver int) func() {
			return func() {
				fs.processDelete(&Msg{Kind: 'D',
					Filename: name,
					Version:  ver})
			}
		}(msg.Filename, fs.gversion)

		fi.timer = time.AfterFunc(dur, timerFunc)
	}
	fi.absexptime = absexptime
	fs.dir[msg.Filename] = fi

	return ok(fs.gversion)
}

func (fs *FS) processWrite(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()
	return fs.internalWrite(msg)
}

func (fs *FS) processCas(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()

	if fi := fs.dir[msg.Filename]; fi != nil {
		if msg.Version != fi.version {
			return &Msg{Kind: 'V', Version: fi.version}
		}
	}
	return fs.internalWrite(msg)
}

func (fs *FS) processDelete(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()
	fi := fs.dir[msg.Filename]
	if fi != nil {
		if msg.Version > 0 && fi.version != msg.Version {
			// non-zero msg.Version indicates a delete due to an expired timer
			return nil // nothing to do
		}
		fi.cancelTimer()
		delete(fs.dir, msg.Filename)
		return ok(0)
	} else {
		return &Msg{Kind: 'F'} // file not found
	}

}

func ok(version int) *Msg {
	return &Msg{Kind: 'O', Version: version}
}
