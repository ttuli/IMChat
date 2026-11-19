package callback

import (
	"bytes"
	"context"
	"crypto"
	"crypto/md5"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// aliOssPubKeyURLPrefix 白名单前缀：Aliyun OSS 回调使用固定的公钥 CDN 域名下发验签公钥。
// 校验解码后的 x-oss-pub-key-url 必须以该前缀开头，可防止攻击者在请求头中指定自控的
// 公钥地址、诱导服务端用攻击者掌握私钥的公钥完成 RSA 验签，从而伪造合法回调。
const (
	aliOssPubKeyURLPrefix  = "https://gosspublic.alicdn.com/"
	aliOssDefaultPubKeyURL = "https://gosspublic.alicdn.com/callback_pub_key_v1.pem"
	// 阿里云 OSS 回调固定公钥（callback_pub_key_v1.pem），启动时预置进缓存，
	// 避免冷启动首次回调在同步链路拉取外网时因跨节点 DNS / 网络抖动触发服务端 3s 超时返回 503。
	aliOssDefaultPubKeyPEM = `-----BEGIN PUBLIC KEY-----
MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKs/JBGzwUB2aVht4crBx3oIPBLNsjGs
C0fTXv+nvlmklvkcolvpvXLTjaxUHR3W9LXxQ2EHXAJfCB+6H2YF1k8CAwEAAQ==
-----END PUBLIC KEY-----`
)

var (
	pubKeyCache   sync.Map
	pubKeyFetcher = &http.Client{Timeout: 2500 * time.Millisecond}
)

func init() {
	if pubKey, err := decodePemPublicKey([]byte(aliOssDefaultPubKeyPEM)); err == nil {
		pubKeyCache.Store(aliOssDefaultPubKeyURL, pubKey)
	}
}

// verifyOssCallback 校验阿里云 OSS 上传回调的 RSA 签名。
// 校验成功返回已读取的原始 body（调用方需重新填回 r.Body 供后续解析）；
// 失败返回具体原因。
func verifyOssCallback(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	_ = r.Body.Close()

	pubKeyURLB64 := r.Header.Get("x-oss-pub-key-url")
	if pubKeyURLB64 == "" {
		return body, errors.New("missing x-oss-pub-key-url header")
	}
	pubKeyURLBytes, err := base64.StdEncoding.DecodeString(pubKeyURLB64)
	if err != nil {
		return body, fmt.Errorf("decode x-oss-pub-key-url: %w", err)
	}
	pubKeyURL := string(pubKeyURLBytes)
	if !strings.HasPrefix(pubKeyURL, aliOssPubKeyURLPrefix) {
		return body, fmt.Errorf("pub key url not on trusted domain: %s", pubKeyURL)
	}

	authB64 := r.Header.Get("authorization")
	if authB64 == "" {
		return body, errors.New("missing authorization header")
	}
	sig, err := base64.StdEncoding.DecodeString(authB64)
	if err != nil {
		return body, fmt.Errorf("decode authorization: %w", err)
	}

	pubKey, err := loadOssPubKey(r.Context(), pubKeyURL)
	if err != nil {
		return body, fmt.Errorf("load pub key: %w", err)
	}

	path, err := url.PathUnescape(r.URL.Path)
	if err != nil {
		return body, fmt.Errorf("unescape path: %w", err)
	}
	var buf bytes.Buffer
	buf.WriteString(path)
	if r.URL.RawQuery != "" {
		buf.WriteByte('?')
		buf.WriteString(r.URL.RawQuery)
	}
	buf.WriteByte('\n')
	buf.Write(body)

	sum := md5.Sum(buf.Bytes())
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.MD5, sum[:], sig); err != nil {
		return body, fmt.Errorf("rsa verify: %w", err)
	}
	return body, nil
}

// loadOssPubKey 从缓存或可信 CDN 获取 OSS 回调公钥，并进程内缓存，避免每次回调都发起外网请求。
func loadOssPubKey(ctx context.Context, pubKeyURL string) (*rsa.PublicKey, error) {
	if v, ok := pubKeyCache.Load(pubKeyURL); ok {
		return v.(*rsa.PublicKey), nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pubKeyURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := pubKeyFetcher.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch pub key http %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	pubKey, err := decodePemPublicKey(raw)
	if err != nil {
		return nil, err
	}
	actual, _ := pubKeyCache.LoadOrStore(pubKeyURL, pubKey)
	return actual.(*rsa.PublicKey), nil
}

func decodePemPublicKey(raw []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("decode pem block")
	}
	return parseRSAPublicKey(block.Bytes)
}

func parseRSAPublicKey(der []byte) (*rsa.PublicKey, error) {
	if pub, err := x509.ParsePKIXPublicKey(der); err == nil {
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("public key is not RSA")
		}
		return rsaPub, nil
	}
	return x509.ParsePKCS1PublicKey(der)
}