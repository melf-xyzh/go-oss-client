/**
 * @Time    :2023/5/23 18:16
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"context"
	"github.com/melf-xyzh/go-oss-client/model"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
	"time"
)

type TencentCloudOss struct {
	Endpoint  string
	SecretId  string
	SecretKey string
	TimeOut   int
	Client    *cos.Client
}

func NewTencentCloudOss(endpoint, secretId, secretKey string, timeOut int) (tencentCloudOss *TencentCloudOss, err error) {
	tencentCloudOss = &TencentCloudOss{
		Endpoint:  endpoint,
		SecretId:  secretId,
		SecretKey: secretKey,
		TimeOut:   timeOut,
	}
	u, _ := url.Parse(endpoint)
	b := &cos.BaseURL{BucketURL: u}
	// 1.永久密钥
	tencentCloudOss.Client = cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretId,  // 用户的 SecretId，建议使用子账号密钥，授权遵循最小权限指引，降低使用风险。子账号密钥获取可参考 https://cloud.tencent.com/document/product/598/37140
			SecretKey: secretKey, // 用户的 SecretKey，建议使用子账号密钥，授权遵循最小权限指引，降低使用风险。子账号密钥获取可参考 https://cloud.tencent.com/document/product/598/37140
		},
	})
	return
}

func (client *TencentCloudOss) NewBucket() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	_, err = client.Client.Bucket.Put(ctx, nil)
	return
}

func (client *TencentCloudOss) RemoveBucket() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	_, err = client.Client.Bucket.Delete(ctx)
	return
}

func (client *TencentCloudOss) BucketExist() (exist bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	exist, err = client.Client.Bucket.IsExist(ctx)
	return
}

func (client *TencentCloudOss) PutObject(objectName string, filePath string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: "application/octet-stream",
		},
	}
	_, err = client.Client.Object.PutFromFile(ctx, objectName, filePath, opt)
	return
}

func (client *TencentCloudOss) GetObject(objectName string, filePath string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	// 下载对象到本地文件
	_, err = client.Client.Object.GetToFile(ctx, objectName, filePath, nil)
	return
}

func (client *TencentCloudOss) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	var marker string
	opt := &cos.BucketGetOptions{
		Prefix:    prefix,     // prefix 表示要查询的文件夹
		Delimiter: startAfter, // deliter 表示分隔符, 设置为/表示列出当前目录下的 object, 设置为空表示列出所有的 object
		MaxKeys:   1000,       // 设置最大遍历出多少个对象, 一次 listobject 最大支持1000
	}
	isTruncated := true
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	for isTruncated {
		opt.Marker = marker
		var v *cos.BucketGetResult
		v, _, err = client.Client.Bucket.Get(ctx, opt)
		if err != nil {
			break
		}
		for _, content := range v.Contents {
			o := ossmod.ObjectInfo{
				Key:          content.Key,
				Size:         content.Size,
				ETag:         content.ETag,
				StorageClass: content.StorageClass,
			}
			o.LastModified, _ = time.Parse("2006-01-02T15:04:05.000Z", content.LastModified)
			objects = append(objects, o)
		}
		isTruncated = v.IsTruncated // 是否还有数据
		marker = v.NextMarker       // 设置下次请求的起始 key
	}
	return
}

func (client *TencentCloudOss) RemoveObject(objectName string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	_, err = client.Client.Object.Delete(ctx, objectName)
	return
}

func (client *TencentCloudOss) ObjectExist(objectName string) (exist bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	exist, err = client.Client.Object.IsExist(ctx, objectName)
	return
}
