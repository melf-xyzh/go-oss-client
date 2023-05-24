/**
 * @Time    :2023/5/23 20:21
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/melf-xyzh/go-oss-client/model"
	"io"
	"os"
)

type HuaweiCloudObs struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Client    *obs.ObsClient
}

func NewHuaweiCloudObs(endpoint, accessKey, secretKey, bucket string) (client *HuaweiCloudObs, err error) {
	client = &HuaweiCloudObs{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
	}
	// 创建ObsClient结构体
	client.Client, err = obs.New(client.AccessKey, client.SecretKey, client.Endpoint)
	return
}

func (client *HuaweiCloudObs) NewBucket() (err error) {
	input := &obs.CreateBucketInput{}
	input.Bucket = client.Bucket
	input.Location = "bucketlocation"
	input.ACL = obs.AclPrivate
	input.StorageClass = obs.StorageClassWarm
	input.AvailableZone = "3az"
	_, err = client.Client.CreateBucket(input)
	return
}

func (client *HuaweiCloudObs) RemoveBucket() (err error) {
	_, err = client.Client.DeleteBucket(client.Bucket)
	return
}

func (client *HuaweiCloudObs) BucketExist() (exist bool, err error) {
	_, err = client.Client.HeadBucket(client.Bucket)
	if err == nil {
		exist = true
	} else {
		exist = false
	}
	return
}

func (client *HuaweiCloudObs) PutObject(objectName string, filePath string) (err error) {
	input := &obs.PutFileInput{}
	input.Bucket = client.Bucket
	input.Key = objectName
	input.SourceFile = filePath
	_, err = client.Client.PutFile(input)
	return
}

func (client *HuaweiCloudObs) GetObject(objectName string, filePath string) (err error) {
	input := &obs.GetObjectInput{}
	input.Bucket = client.Bucket
	input.Key = objectName
	var output *obs.GetObjectOutput
	output, err = client.Client.GetObject(input)
	if err == nil {
		defer output.Body.Close()
		var file *os.File
		file, err = os.Create(filePath)
		if err != nil {
			return
		}
		defer file.Close()
		// 拷贝文件
		_, err = io.Copy(file, output.Body)
	}
	return
}

func (client *HuaweiCloudObs) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	input := &obs.ListObjectsInput{}
	input.Bucket = client.Bucket
	input.Prefix = prefix
	input.Marker = startAfter
	var output *obs.ListObjectsOutput
	output, err = client.Client.ListObjects(input)
	if err != nil {
		return
	}
	for _, val := range output.Contents {
		o := ossmod.ObjectInfo{
			Key:          val.Key,
			Size:         val.Size,
			ETag:         val.ETag,
			LastModified: val.LastModified,
			StorageClass: string(val.StorageClass),
		}
		objects = append(objects, o)
	}
	return
}

func (client *HuaweiCloudObs) RemoveObject(objectName string) (err error) {
	input := &obs.DeleteObjectInput{}
	input.Bucket = client.Bucket
	input.Key = objectName
	_, err = client.Client.DeleteObject(input)
	return
}

func (client *HuaweiCloudObs) ObjectExist(objectName string) (exist bool, err error) {
	input := &obs.GetObjectMetadataInput{}
	input.Bucket = client.Bucket
	input.Key = objectName
	_, err = client.Client.GetObjectMetadata(input)
	if err != nil {
		exist = false
	} else {
		exist = true
	}
	return
}
