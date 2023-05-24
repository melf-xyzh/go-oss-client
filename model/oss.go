/**
 * @Time    :2023/5/23 16:50
 * @Author  :Xiaoyu.Zhang
 */

package ossmod

import (
	"time"
)

type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	StorageClass string
}
