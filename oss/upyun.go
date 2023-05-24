/**
 * @Time    :2023/5/23 20:21
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"github.com/melf-xyzh/go-oss-client/model"
	"github.com/upyun/go-sdk/v3/upyun"
	"strings"
)

type UpYunOss struct {
	//Endpoint        string
	Operator string
	Password string
	Bucket   string
	TimeOut  int
	Client   *upyun.UpYun
}

func NewUpYunOss(operator, password, bucket string) (client *UpYunOss) {
	client = &UpYunOss{
		Operator: operator,
		Password: password,
		Bucket:   bucket,
	}
	// 初始化又拍云
	client.Client = upyun.NewUpYun(&upyun.UpYunConfig{
		Bucket:   client.Bucket,
		Operator: client.Operator,
		Password: client.Password,
	})
	return
}

func (client *UpYunOss) NewBucket() (err error) {
	return nil
}

func (client *UpYunOss) RemoveBucket() (err error) {
	return nil
}

func (client *UpYunOss) BucketExist() (exist bool, err error) {
	return true, err
}

func (client *UpYunOss) PutObject(objectName string, filePath string) (err error) {
	// 上传文件
	err = client.Client.Put(&upyun.PutObjectConfig{
		Path:      objectName,
		LocalPath: filePath,
	})
	return
}

func (client *UpYunOss) GetObject(objectName string, filePath string) (err error) {
	_, err = client.Client.Get(&upyun.GetObjectConfig{
		Path:      objectName,
		LocalPath: filePath,
	})
	return
}

func (client *UpYunOss) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	// 列目录
	objsChan := make(chan *upyun.FileInfo, 10)
	go func() {
		err = client.Client.List(&upyun.GetObjectsConfig{
			Path:        startAfter,
			ObjectsChan: objsChan,
		})
	}()
	for obj := range objsChan {
		if strings.HasPrefix(obj.Name, prefix) {
			o := ossmod.ObjectInfo{
				Key:          obj.Name,
				Size:         obj.Size,
				LastModified: obj.Time,
			}
			objects = append(objects, o)
		}
	}
	return
}

func (client *UpYunOss) RemoveObject(objectName string) (err error) {
	err = client.Client.Delete(&upyun.DeleteObjectConfig{
		Path: objectName,
	})
	return
}

func (client *UpYunOss) ObjectExist(objectName string) (exist bool, err error) {
	_, err = client.Client.GetInfo(objectName)
	if err != nil {
		exist = false
		return
	}
	exist = true
	return
}
