/**
 * @Time    :2023/5/24 11:30
 * @Author  :Xiaoyu.Zhang
 */

package fsnotify

import (
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"log"
	"os"
	"path/filepath"
)

// 参考文档
// https://zhuanlan.zhihu.com/p/425106085
// https://www.topgoer.com/%E5%85%B6%E4%BB%96/%E6%96%87%E4%BB%B6%E7%9B%91%E6%8E%A7.html

type Event struct {
	fsnotify.Event
}

// FileWatch 定义接口，调用文件监控必须实现此接口
type FileWatch interface {
	InitCallback(dir string) (err error)
	CreateCallback(ev Event)
	WriteCallback(ev Event)
	RemoveCallback(ev Event)
	RenameCallback(ev Event)
	ChmodCallback(ev Event)
	OtherCallback(ev Event)
}

type Watch struct {
	watch *fsnotify.Watcher
}

func NewWatch(callBack FileWatch, dirs ...string) (w Watch, err error) {
	var watch *fsnotify.Watcher
	watch, err = fsnotify.NewWatcher()
	if err != nil {
		return
	}
	w = Watch{watch: watch}
	// 监听注册文件夹
	for _, dir := range dirs {
		err = w.watchDir(dir, callBack)
		if err != nil {
			break
		}
	}
	return
}

func (w *Watch) walkPath(path string, info os.FileInfo, err error) error {
	if err == nil {
		return err
	}
	if info == nil {
		err = errors.New("the file info is nil")
		return err
	}
	// 判断是否为文件夹
	if info.IsDir() {
		// 返回该路径的绝对路径
		path, err = filepath.Abs(path)
		if err != nil {
			return err
		}
		// 将此路径加入监听
		err = w.watch.Add(path)
		if err != nil {
			return err
		}
	}
	return nil
}

// watchDir
/**
 *  @Description: 监听文件夹
 *  @receiver w
 *  @param dir
 *  @param callback
 *  @return err
 */
func (w *Watch) watchDir(dir string, callback FileWatch) (err error) {
	//// 遍历指定目录下的所有文件
	//err = filepath.Walk(dir, w.walkPath)
	//if err != nil {
	//	return
	//}

	err = w.watch.Add(dir)
	if err != nil {
		return
	}
	log.Println("监控服务已经启动", dir)
	// 初始化回调
	err = callback.InitCallback(dir)
	if err != nil {
		log.Panicln(err.Error())
		return err
	}
	go func() {
		for {
			select {
			case e, ok := <-w.watch.Events:
				if !ok {
					log.Panicln("e, ok := <-w.watch.Events")
					return
				}
				log.Println(fmt.Sprintf("监听到文件 %s 变化| ", e.Name))
				switch e.Op {
				case fsnotify.Create:
					fi, err := os.Stat(e.Name)
					if err == nil && fi.IsDir() {
						w.watch.Add(e.Name)
					}
					log.Println("添加监控:", e.Name)
					callback.CreateCallback(Event{e})
				case fsnotify.Write:
					callback.WriteCallback(Event{e})
				case fsnotify.Remove:
					fi, err := os.Stat(e.Name)
					if err == nil && fi.IsDir() {
						w.watch.Remove(e.Name)
						log.Println("删除监控 : ", e.Name)
					}
					callback.RemoveCallback(Event{e})
				case fsnotify.Rename:
					callback.RenameCallback(Event{e})
					w.watch.Remove(e.Name)
					fmt.Println("重命名文件 : ", e.Name)
				case fsnotify.Chmod:
					callback.RenameCallback(Event{e})
				default:
					callback.OtherCallback(Event{e})
				}
			case err, ok := <-w.watch.Errors:
				if !ok {
					log.Panicln("err, ok := <-w.watch.Errors")
					return
				}
				log.Println("error:", err)
			}
		}
	}()
	return
}
