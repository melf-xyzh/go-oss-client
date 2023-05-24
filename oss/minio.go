/**
 * @Time    :2023/5/23 16:05
 * @Author  :Xiaoyu.Zhang
 */

package oss

import (
	"context"
	"github.com/melf-xyzh/go-oss-client/model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log"
	"time"
)

type MinioOss struct {
	Endpoint        string
	AccessKeyId     string
	AccessKeySecret string
	Bucket          string
	TimeOut         int
	Client          *minio.Client
}

func NewMinioOss(endpoint, accessKeyId, accessKeySecret, bucket string, timeOut int, useSSL bool) (minioOss *MinioOss, err error) {
	minioOss = &MinioOss{
		Endpoint:        endpoint,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Bucket:          bucket,
		TimeOut:         timeOut,
	}
	// 初始化minio
	minioOss.Client, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyId, accessKeySecret, ""),
		Secure: useSSL,
	})
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

func (client *MinioOss) BucketExist() (exist bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	exist, err = client.Client.BucketExists(ctx, client.Bucket)
	return
}

// NewBucket
/**
 *  @Description: 创建存储桶
 *  @receiver client
 *  @param bucketName
 *  @return err
 */
func (client *MinioOss) NewBucket() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	location := "us-east-1"
	err = client.Client.MakeBucket(ctx, client.Bucket, minio.MakeBucketOptions{Region: location})
	return
}

// RemoveBucket
/**
 *  @Description: 删除存储桶
 *  @receiver client
 *  @param bucketName
 *  @return err
 */
func (client *MinioOss) RemoveBucket() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	err = client.Client.RemoveBucket(ctx, client.Bucket)
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
func (client *MinioOss) ListObjects(prefix, startAfter string) (objects []ossmod.ObjectInfo, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	opts := minio.ListObjectsOptions{}
	// 设置指定前缀
	if prefix != "" {
		opts.Prefix = prefix
		opts.Recursive = true
	}
	// 设置指定起始位置
	if startAfter != "" {
		opts.StartAfter = startAfter
		opts.UseV1 = true
	}

	objectCh := client.Client.ListObjects(ctx, client.Bucket, opts)
	// 遍历Object
	for object := range objectCh {
		if object.Err != nil {
			err = object.Err
			return
		}
		o := ossmod.ObjectInfo{
			Key:          object.Key,
			Size:         object.Size,
			ETag:         object.ETag,
			LastModified: object.LastModified,
			StorageClass: object.StorageClass,
		}
		objects = append(objects, o)
	}
	return
}

// PutObject
/**
 *  @Description: 上传文件
 *  @receiver client
 *  @param bucketName 存储桶
 *  @param objectName Object的完整路径
 *  @param filePath 本地文件的路径
 *  @return err
 */
func (client *MinioOss) PutObject(objectName, filePath string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	_, err = client.Client.FPutObject(ctx, client.Bucket, objectName, filePath, opts)
	return
}

// GetObject
/**
 *  @Description: 下载文件
 *  @receiver client
 *  @param bucketName 存储桶
 *  @param objectName Object的完整路径
 *  @param filePath 本地文件的路径
 *  @return err
 */
func (client *MinioOss) GetObject(objectName, filePath string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	err = client.Client.FGetObject(ctx, client.Bucket, objectName, filePath, minio.GetObjectOptions{})
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
func (client *MinioOss) RemoveObject(objectName string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true, // 使用该参数会忽略所有 Bucket 的生命周期配置、对象锁定配置以及任何其他的数据保留规则，直接删除对象。
	}
	// 删除单个文件
	err = client.Client.RemoveObject(ctx, client.Bucket, objectName, opts)
	if err != nil {
		log.Fatalln(err)
	}
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
func (client *MinioOss) ObjectExist(objectName string) (exist bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(client.TimeOut)*time.Second)
	defer cancel()
	_, err = client.Client.StatObject(ctx, client.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		exist = false
		return
	}
	exist = true
	return
}
