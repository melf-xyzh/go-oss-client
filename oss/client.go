/**
 * @Time    :2023/5/23 15:57
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"fmt"
	"github.com/melf-xyzh/go-oss-client/model"
	"github.com/qiniu/go-sdk/v7/storage"
)

type ClientI interface {
	// NewBucket 创建存储桶
	NewBucket() (err error)
	// RemoveBucket 删除存储桶
	RemoveBucket() (err error)
	// BucketExist 判断存储桶是否存在
	BucketExist() (exist bool, err error)
	// PutObject 上传对象
	PutObject(objectName string, filePath string) (err error)
	// GetObject 下载对象
	GetObject(objectName string, filePath string) (err error)
	// ListObjects 列出对象
	ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error)
	// RemoveObject 删除对象
	RemoveObject(objectName string) (err error)
	// ObjectExist 判断对象是否存在
	ObjectExist(objectName string) (exist bool, err error)
}

func NewClient(name string) (client ClientI, err error) {
	switch name {
	case "aliyun":
		client, err = NewALiYunOss("", "", "", "")
	case "tencent":
		client, err = NewTencentCloudOss("", "", "", 15)
	case "minio":
		client, err = NewMinioOss("", "", "", "", 15, false)
	case "qiniu":
		client = NewQiNiuCloudOss("", "", "", "", 15, false, storage.RIDHuadong)
	case "upyun":
		client = NewUpYunOss("", "", "")
	case "baidu":
		client, err = NewBaiduCloudBos("", "", "", "")
	case "huawei":
		client, err = NewHuaweiCloudObs("", "", "", "")
	}
	return
}

type Template struct {
	Client ClientI
}

func (ossTem *Template) Upload(objectName, filePath string) (err error) {
	// 判断存储桶是否存在
	var exist bool
	exist, err = ossTem.Client.BucketExist()
	if err != nil {
		return
	}
	// 若不存在则创建存储桶
	if !exist {
		err = ossTem.Client.NewBucket()
		if err != nil {
			return err
		}
	}
	// 判断文件是否存在
	exist, _ = ossTem.Client.ObjectExist(objectName)
	if exist {
		fmt.Println("已存在，无需重复上送")
		return
	}
	// 上传对象
	err = ossTem.Client.PutObject(objectName, filePath)
	if err != nil {
		fmt.Println("上送失败", err.Error())
		// 删除上送对象
		err = ossTem.Client.RemoveObject(objectName)
		if err != nil {
			fmt.Println("删除失败", err.Error())
		}
	} else {
		fmt.Println("上送", objectName, "成功")
	}
	return
}
