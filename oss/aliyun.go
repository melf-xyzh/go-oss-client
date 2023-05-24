/**
 * @Time    :2023/5/23 16:05
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/melf-xyzh/go-oss-client/model"
)

type ALiYunOss struct {
	Endpoint        string
	AccessKeyId     string
	AccessKeySecret string
	Bucket          string
	Client          *oss.Client
}

func NewALiYunOss(endpoint, accessKeyId, accessKeySecret, bucket string) (aLiYunOss *ALiYunOss, err error) {
	aLiYunOss = &ALiYunOss{
		Endpoint:        endpoint,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Bucket:          bucket,
	}
	aLiYunOss.Client, err = oss.New(endpoint, accessKeyId, accessKeySecret)
	if err != nil {
		return
	}
	return
}

// BucketExist
/**
 *  @Description: 判断存储桶是否存在
 *  @receiver client
 *  @param bucketName
 *  @return exist
 *  @return err
 */
func (client *ALiYunOss) BucketExist() (exist bool, err error) {
	exist, err = client.Client.IsBucketExist(client.Bucket)
	return
}

// NewBucket
/**
 *  @Description: 创建存储桶
 *  @receiver client
 *  @param bucketName
 *  @return err
 */
func (client *ALiYunOss) NewBucket() (err error) {
	err = client.Client.CreateBucket(client.Bucket)
	return
}

func (client *ALiYunOss) RemoveBucket() (err error) {
	err = client.Client.DeleteBucket(client.Bucket)
	return
}

// ListObjects
/**
 *  @Description: 获取对象列表
 *  @receiver client
 *  @param bucketName
 *  @param prefix
 *  @param startAfter
 *  @return objects
 *  @return err
 */
func (client *ALiYunOss) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	var bucket *oss.Bucket
	// 获取存储桶
	bucket, err = client.Client.Bucket(client.Bucket)
	if err != nil {
		return
	}
	// 分页列举包含指定前缀的文件。每页列举100个。
	prefixOptions := oss.Prefix(prefix)
	markerOptions := oss.Marker(startAfter)
	for {
		var lsRes oss.ListObjectsResult
		lsRes, err = bucket.ListObjects(oss.MaxKeys(100), markerOptions, prefixOptions)
		if err != nil {
			return
		}

		for _, object := range lsRes.Objects {
			o := ossmod.ObjectInfo{
				Key:          object.Key,
				Size:         object.Size,
				ETag:         object.ETag,
				LastModified: object.LastModified,
				StorageClass: object.StorageClass,
			}
			objects = append(objects, o)
		}
		//// 打印列举结果。默认情况下，一次返回100条记录。
		//fmt.Println("Objects:", lsRes.Objects)
		if lsRes.IsTruncated {
			prefixOptions = oss.Prefix(lsRes.Prefix)
			markerOptions = oss.Marker(lsRes.NextMarker)
		} else {
			break
		}
	}
	return
}

// PutObject
/**
 *  @Description: 上传文件
 *  @receiver client
 *  @param bucketName 存储桶
 *  @param objectName Object的完整路径
 *  @param filePath 本地文件的完整路径
 *  @return err
 */
func (client *ALiYunOss) PutObject(objectName, filePath string) (err error) {
	var bucket *oss.Bucket
	// 获取存储桶
	bucket, err = client.Client.Bucket(client.Bucket)
	if err != nil {
		return
	}
	// 上传本地文件
	// 依次填写Object的完整路径（例如exampledir/exampleobject.txt）和本地文件的完整路径（例如D:\\localpath\\examplefile.txt）。
	err = bucket.PutObjectFromFile(objectName, filePath)
	return
}

// GetObject
/**
 *  @Description: 下载文件
 *  @receiver client
 *  @param bucketName 存储桶
 *  @param objectName Object的完整路径
 *  @param filePath 本地文件的完整路径
 *  @return err
 */
func (client *ALiYunOss) GetObject(objectName, filePath string) (err error) {
	var bucket *oss.Bucket
	// 获取存储桶
	bucket, err = client.Client.Bucket(client.Bucket)
	if err != nil {
		return
	}
	// 下载到本地
	// 依次填写Object的完整路径（例如exampledir/exampleobject.txt）和本地文件的完整路径（例如D:\\localpath\\examplefile.txt）。
	err = bucket.GetObjectToFile(objectName, filePath)
	return
}

// RemoveObject
/**
 *  @Description: 删除单个文件
 *  @receiver client
 *  @param bucketName
 *  @param objectName
 *  @return err
 */
func (client *ALiYunOss) RemoveObject(objectName string) (err error) {
	var bucket *oss.Bucket
	// 获取存储桶
	bucket, err = client.Client.Bucket(client.Bucket)
	if err != nil {
		return
	}
	// 删除单个文件
	err = bucket.DeleteObject(objectName)
	return
}

// ObjectExist
/**
 *  @Description: 判断文件是否存在
 *  @receiver client
 *  @param bucketName
 *  @param objectName 不包含Bucket名称在内的Object的完整路径
 *  @return exist
 *  @return err
 */
func (client *ALiYunOss) ObjectExist(objectName string) (exist bool, err error) {
	var bucket *oss.Bucket
	// 获取存储桶
	bucket, err = client.Client.Bucket(client.Bucket)
	if err != nil {
		return
	}
	// 判断文件是否存在。
	exist, err = bucket.IsObjectExist(objectName)
	return
}
