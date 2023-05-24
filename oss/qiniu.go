/**
 * @Time    :2023/5/23 18:16
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"context"
	"github.com/melf-xyzh/go-oss-client/model"
	"github.com/qiniu/go-sdk/v7/auth/qbox"
	"github.com/qiniu/go-sdk/v7/storage"
	"io"
	"net/http"
	"os"
	"time"
)

type QiNiuCloudOss struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Bucket        string
	TimeOut       int
	mac           *qbox.Mac
	putPolicy     storage.PutPolicy
	bucketManager *storage.BucketManager
	RegionID      storage.RegionID
}

func NewQiNiuCloudOss(endpoint, accessKey, secretKey, bucket string, timeOut int, useSSL bool, regionID storage.RegionID) (qiNiuCloudOss *QiNiuCloudOss) {
	qiNiuCloudOss = &QiNiuCloudOss{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		TimeOut:   timeOut,
		RegionID:  regionID,
	}
	qiNiuCloudOss.mac = qbox.NewMac(accessKey, secretKey)
	qiNiuCloudOss.putPolicy = storage.PutPolicy{
		Scope: bucket,
	}

	cfg := storage.Config{
		// 是否使用https域名进行资源管理
		UseHTTPS: useSSL,
	}
	// 指定空间所在的区域，如果不指定将自动探测
	// 如果没有特殊需求，默认不需要指定
	//cfg.Region=&storage.ZoneHuabei
	qiNiuCloudOss.bucketManager = storage.NewBucketManager(qiNiuCloudOss.mac, &cfg)
	return
}

func (client *QiNiuCloudOss) NewBucket() (err error) {
	err = client.bucketManager.CreateBucket(client.Bucket, client.RegionID)
	return
}

func (client *QiNiuCloudOss) RemoveBucket() (err error) {
	err = client.bucketManager.DropBucket(client.Bucket)
	return
}

func (client *QiNiuCloudOss) BucketExist() (exist bool, err error) {
	_, err = client.bucketManager.GetBucketInfo(client.Bucket)
	if err != nil {
		exist = false
		return
	}
	exist = true
	return
}

func (client *QiNiuCloudOss) PutObject(objectName string, filePath string) (err error) {
	// 进行上传凭证的生成
	upToken := client.putPolicy.UploadToken(client.mac)
	cfg := storage.Config{}
	// 用来构建一个表单上传的对象
	formUploader := storage.NewFormUploader(&cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	err = formUploader.PutFile(ctx, nil, upToken, objectName, filePath, nil)
	return
}

func (client *QiNiuCloudOss) GetObject(objectName string, filePath string) (err error) {
	deadline := time.Now().Add(time.Second * 3600).Unix() //1小时有效期
	privateAccessURL := storage.MakePrivateURL(client.mac, client.Endpoint, objectName, deadline)
	// 使用http下载对象
	var resp *http.Response
	resp, err = http.Get(privateAccessURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var file *os.File
	file, err = os.Create(filePath)
	if err != nil {
		return
	}
	defer file.Close()
	// 拷贝文件
	_, err = io.Copy(file, resp.Body)
	return
}

func (client *QiNiuCloudOss) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	limit := 1000
	delimiter := ""
	//初始列举marker为空
	marker := startAfter
	for {
		var entries []storage.ListItem
		var nextMarker string
		var hasNext bool
		entries, _, nextMarker, hasNext, err = client.bucketManager.ListFiles(client.Bucket, prefix, delimiter, marker, limit)
		if err != nil {
			break
		}
		for _, entry := range entries {
			o := ossmod.ObjectInfo{
				Key:  entry.Key,
				Size: entry.Fsize,
			}
			o.LastModified = time.Unix(entry.PutTime, 0)
			objects = append(objects, o)
		}
		if hasNext {
			marker = nextMarker
		} else {
			break
		}
	}
	return
}

func (client *QiNiuCloudOss) RemoveObject(objectName string) (err error) {
	err = client.bucketManager.Delete(client.Bucket, objectName)
	return
}

func (client *QiNiuCloudOss) ObjectExist(objectName string) (exist bool, err error) {
	_, err = client.bucketManager.Stat(client.Bucket, objectName)
	if err != nil {
		exist = false
		return
	}
	exist = true
	return
}
