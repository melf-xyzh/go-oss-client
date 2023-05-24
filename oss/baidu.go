/**
 * @Time    :2023/5/23 20:21
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/melf-xyzh/go-oss-client/model"
	"time"
)

type BaiduCloudBos struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Client    *bos.Client
}

func NewBaiduCloudBos(endpoint, accessKey, secretKey, bucket string) (client *BaiduCloudBos, err error) {
	client = &BaiduCloudBos{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
		Client:    nil,
	}
	clientConfig := bos.BosClientConfiguration{
		Ak:               accessKey,
		Sk:               secretKey,
		Endpoint:         endpoint,
		RedirectDisabled: false,
	}
	// 初始化一个BosClient
	client.Client, err = bos.NewClientWithConfig(&clientConfig)
	return
}

func (client *BaiduCloudBos) NewBucket() (err error) {
	_, err = client.Client.PutBucket(client.Bucket)
	return
}

func (client *BaiduCloudBos) RemoveBucket() (err error) {
	err = client.Client.DeleteBucket(client.Bucket)
	return
}

func (client *BaiduCloudBos) BucketExist() (exist bool, err error) {
	exist, err = client.Client.DoesBucketExist(client.Bucket)
	return
}

func (client *BaiduCloudBos) PutObject(objectName string, filePath string) (err error) {
	_, err = client.Client.PutObjectFromFile(client.Bucket, objectName, filePath, nil)
	return
}

func (client *BaiduCloudBos) GetObject(objectName string, filePath string) (err error) {
	err = client.Client.BasicGetObjectToFile(client.Bucket, objectName, filePath)
	return
}

func (client *BaiduCloudBos) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	args := &api.ListObjectsArgs{
		Marker: startAfter,
		Prefix: prefix,
	}
	var listObjectResult *api.ListObjectsResult
	listObjectResult, err = client.Client.ListObjects(client.Bucket, args)
	if err != nil {
		return
	}
	// 打印Contents字段的具体结果
	for _, obj := range listObjectResult.Contents {
		o := ossmod.ObjectInfo{
			Key:          obj.Key,
			Size:         int64(obj.Size),
			ETag:         obj.ETag,
			StorageClass: obj.StorageClass,
		}
		o.LastModified, _ = time.Parse("2006-01-02T15:04:05Z", obj.LastModified)
		objects = append(objects, o)
	}
	return
}

func (client *BaiduCloudBos) RemoveObject(objectName string) (err error) {
	err = client.Client.DeleteObject(client.Bucket, objectName)
	return
}

func (client *BaiduCloudBos) ObjectExist(objectName string) (exist bool, err error) {
	_, err = client.Client.GetObjectMeta(client.Bucket, objectName)
	if realErr, ok := err.(*bce.BceServiceError); ok {
		if realErr.StatusCode == 404 {
			exist = false
			return
		}
	}
	exist = true
	return
}
