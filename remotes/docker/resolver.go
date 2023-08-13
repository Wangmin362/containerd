/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker/schema1" //nolint:staticcheck // Ignore SA1019. Need to keep deprecated package for compatibility.
	remoteerrors "github.com/containerd/containerd/remotes/errors"
	"github.com/containerd/containerd/tracing"
	"github.com/containerd/containerd/version"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	// ErrInvalidAuthorization is used when credentials are passed to a server but
	// those credentials are rejected.
	ErrInvalidAuthorization = errors.New("authorization failed")

	// MaxManifestSize represents the largest size accepted from a registry
	// during resolution. Larger manifests may be accepted using a
	// resolution method other than the registry.
	//
	// NOTE: The max supported layers by some runtimes is 128 and individual
	// layers will not contribute more than 256 bytes, making a
	// reasonable limit for a large image manifests of 32K bytes.
	// 4M bytes represents a much larger upper bound for images which may
	// contain large annotations or be non-images. A proper manifest
	// design puts large metadata in subobjects, as is consistent the
	// intent of the manifest design.
	MaxManifestSize int64 = 4 * 1048 * 1048
)

// Authorizer is used to authorize HTTP requests based on 401 HTTP responses.
// An Authorizer is responsible for caching tokens or credentials used by
// requests.
type Authorizer interface {
	// Authorize sets the appropriate `Authorization` header on the given
	// request.
	//
	// If no authorization is found for the request, the request remains
	// unmodified. It may also add an `Authorization` header as
	//  "bearer <some bearer token>"
	//  "basic <base64 encoded credentials>"
	//
	// It may return remotes/errors.ErrUnexpectedStatus, which for example,
	// can be used by the caller to find out the status code returned by the registry.
	Authorize(context.Context, *http.Request) error

	// AddResponses adds a 401 response for the authorizer to consider when
	// authorizing requests. The last response should be unauthorized and
	// the previous requests are used to consider redirects and retries
	// that may have led to the 401.
	//
	// If response is not handled, returns `ErrNotImplemented`
	AddResponses(context.Context, []*http.Response) error
}

// ResolverOptions are used to configured a new Docker register resolver
type ResolverOptions struct {
	// Hosts returns registry host configurations for a namespace.
	// 代表了当前请求的镜像仓库的客户端配置，譬如如何认证、URL是啥，Http还是HTTPS，请求头是啥，当前镜像仓库所支持的能力
	Hosts RegistryHosts

	// Headers are the HTTP request header fields sent by the resolver
	Headers http.Header

	// Tracker is used to track uploads to the registry. This is used
	// since the registry does not have upload tracking and the existing
	// mechanism for getting blob upload status is expensive.
	Tracker StatusTracker

	// Authorizer is used to authorize registry requests
	//
	// Deprecated: use Hosts.
	Authorizer Authorizer

	// Credentials provides username and secret given a host.
	// If username is empty but a secret is given, that secret
	// is interpreted as a long lived token.
	//
	// Deprecated: use Hosts.
	Credentials func(string) (string, string, error)

	// Host provides the hostname given a namespace.
	//
	// Deprecated: use Hosts.
	Host func(string) (string, error)

	// PlainHTTP specifies to use plain http and not https
	//
	// Deprecated: use Hosts.
	PlainHTTP bool

	// Client is the http client to used when making registry requests
	//
	// Deprecated: use Hosts.
	Client *http.Client
}

// DefaultHost is the default host function.
func DefaultHost(ns string) (string, error) {
	if ns == "docker.io" {
		return "registry-1.docker.io", nil
	}
	return ns, nil
}

// dockerResolver实际上就是镜像仓库的客户端工具，只不过里面可以针对同一个镜像，配置多个镜像仓库下载地址，但是优先会按照定义的顺序下载镜像
type dockerResolver struct {
	hosts         RegistryHosts
	header        http.Header
	resolveHeader http.Header
	tracker       StatusTracker
}

// NewResolver returns a new resolver to a Docker registry
func NewResolver(options ResolverOptions) remotes.Resolver {
	if options.Tracker == nil {
		options.Tracker = NewInMemoryTracker()
	}

	// 这里的Header应该就是通过在/etc/containerd/certs.d/<host>/config.toml文件中获取到的header
	if options.Headers == nil {
		options.Headers = make(http.Header)
	}
	if _, ok := options.Headers["User-Agent"]; !ok {
		options.Headers.Set("User-Agent", "containerd/"+version.Version)
	}

	resolveHeader := http.Header{}
	if _, ok := options.Headers["Accept"]; !ok {
		// set headers for all the types we support for resolution.
		resolveHeader.Set("Accept", strings.Join([]string{
			// 下面两个是docker自定义的MediaType
			images.MediaTypeDockerSchema2Manifest,
			images.MediaTypeDockerSchema2ManifestList,
			// 下面两个是OCI自定义的MediaType
			ocispec.MediaTypeImageManifest,
			ocispec.MediaTypeImageIndex, "*/*",
		}, ", "))
	} else {
		resolveHeader["Accept"] = options.Headers["Accept"]
		delete(options.Headers, "Accept")
	}

	// 配置默认的镜像解析器
	if options.Hosts == nil {
		var opts []RegistryOpt
		if options.Host != nil {
			opts = append(opts, WithHostTranslator(options.Host))
		}

		if options.Authorizer == nil {
			options.Authorizer = NewDockerAuthorizer(
				WithAuthClient(options.Client),
				WithAuthHeader(options.Headers),
				WithAuthCreds(options.Credentials))
		}
		opts = append(opts, WithAuthorizer(options.Authorizer))

		if options.Client != nil {
			opts = append(opts, WithClient(options.Client))
		}
		if options.PlainHTTP {
			opts = append(opts, WithPlainHTTP(MatchAllHosts))
		} else {
			opts = append(opts, WithPlainHTTP(MatchLocalhost))
		}
		options.Hosts = ConfigureDefaultRegistries(opts...)
	}
	return &dockerResolver{
		hosts:         options.Hosts,
		header:        options.Headers,
		resolveHeader: resolveHeader,
		tracker:       options.Tracker,
	}
}

func getManifestMediaType(resp *http.Response) string {
	// Strip encoding data (manifests should always be ascii JSON)
	contentType := resp.Header.Get("Content-Type")
	if sp := strings.IndexByte(contentType, ';'); sp != -1 {
		contentType = contentType[0:sp]
	}

	// As of Apr 30 2019 the registry.access.redhat.com registry does not specify
	// the content type of any data but uses schema1 manifests.
	if contentType == "text/plain" {
		contentType = images.MediaTypeDockerSchema1Manifest
	}
	return contentType
}

type countingReader struct {
	reader    io.Reader
	bytesRead int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

var _ remotes.Resolver = &dockerResolver{}

// Resolve
// 1、ref为镜像名
// 2、返回值为OCI组织定义的标准描述符
// 3、该方法主要用于向镜像仓库获取当前要下载镜像的摘要信息
func (r *dockerResolver) Resolve(ctx context.Context, ref string) (string, ocispec.Descriptor, error) {
	// 通过传入的镜像名，解析镜像，获取可以下载当前镜像的镜像仓库的客户端配置
	base, err := r.resolveDockerBase(ref)
	if err != nil {
		return "", ocispec.Descriptor{}, err
	}
	refspec := base.refspec
	if refspec.Object == "" {
		return "", ocispec.Descriptor{}, reference.ErrObjectRequired
	}

	var (
		firstErr error
		paths    [][]string
		dgst     = refspec.Digest()
		caps     = HostCapabilityPull
	)

	// 1、通常情况下，下载镜像都是通过tag的方式下载镜像，譬如：ctr --debug image pull --hosts-dir=/etc/containerd/certs.d registry.k8s.io/sig-storage/csi-provisioner:v3.5.0
	// 这种情况下解析出来的镜像就没有摘要
	// 2、但是我们也可以通过摘要镜像下载，譬如：ctr --debug image pull --hosts-dir=/etc/containerd/certs.d registry.k8s.io/sig-storage/csi-provisioner@sha256:d078dc174323407e8cc6f0f9abd4efaac5db27838f1564d0253d5e3233e3f17f
	// 这种情况下，解析出来的镜像就有摘要
	if dgst != "" {
		// 校验摘要是否有效
		if err := dgst.Validate(); err != nil {
			// need to fail here, since we can't actually resolve the invalid
			// digest.
			return "", ocispec.Descriptor{}, err
		}

		// turns out, we have a valid digest, make a url.
		paths = append(paths, []string{"manifests", dgst.String()})

		// fallback to blobs on not found.
		paths = append(paths, []string{"blobs", dgst.String()})
	} else {
		// Add，如果没有解析到摘要，那么refspec.Object肯定是镜像的Tag
		paths = append(paths, []string{"manifests", refspec.Object})
		caps |= HostCapabilityResolve
	}

	// 过滤出有拉取镜像能力的仓库
	hosts := base.filterHosts(caps)
	if len(hosts) == 0 {
		return "", ocispec.Descriptor{}, fmt.Errorf("no resolve hosts: %w", errdefs.ErrNotFound)
	}

	ctx, err = ContextWithRepositoryScope(ctx, refspec, false)
	if err != nil {
		return "", ocispec.Descriptor{}, err
	}

	for _, u := range paths {
		// 1、根据/etc/containerd/certs.d/<host>/config.yaml配置，按照定义的顺序，进行镜像的请求
		// 2、所以，我们应该尽可能的把速度最快的镜像仓库放在最前面，也就是说hosts配置应该按照镜像仓的速度优先进行定义
		for _, host := range hosts {
			ctx := log.WithLogger(ctx, log.G(ctx).WithField("host", host.Host))

			// 1、向镜像仓库发出HEAD请求，拉取manifests
			// 发出类似https://registry-1.docker.io/v2/library/redis/manifests/6.2.13-alpine的请求
			// 或者：https://k8s.m.daocloud.io/v2/sig-storage/csi-provisioner/manifests/v3.5.0?ns=registry.k8s.io
			req := base.request(host, http.MethodHead, u...)
			if err := req.addNamespace(base.refspec.Hostname()); err != nil {
				return "", ocispec.Descriptor{}, err
			}

			// 设置请求头
			for key, value := range r.resolveHeader {
				req.header[key] = append(req.header[key], value...)
			}

			log.G(ctx).Debug("resolving")
			// 请求镜像仓库
			resp, err := req.doWithRetries(ctx, nil)
			if err != nil {
				if errors.Is(err, ErrInvalidAuthorization) {
					err = fmt.Errorf("pull access denied, repository does not exist or may require authorization: %w", err)
				}
				// Store the error for referencing later
				if firstErr == nil {
					firstErr = err
				}
				log.G(ctx).WithError(err).Info("trying next host")
				continue // try another host
			}
			resp.Body.Close() // don't care about body contents.

			if resp.StatusCode > 299 {
				if resp.StatusCode == http.StatusNotFound {
					log.G(ctx).Info("trying next host - response was http.StatusNotFound")
					continue
				}
				if resp.StatusCode > 399 {
					// Set firstErr when encountering the first non-404 status code.
					if firstErr == nil {
						firstErr = remoteerrors.NewUnexpectedStatusErr(resp)
					}
					continue // try another host
				}
				return "", ocispec.Descriptor{}, remoteerrors.NewUnexpectedStatusErr(resp)
			}
			// 响应值的大小， TODO 对于Head请求，这里的size肯定是等于0的
			size := resp.ContentLength
			// 获取镜像仓库的响应MediaType
			contentType := getManifestMediaType(resp)

			// if no digest was provided, then only a resolve
			// trusted registry was contacted, in this case use
			// the digest header (or content from GET)
			// 如果镜像拉取的时候使用的是Tag,那么这里一定是空的，因为dgst是从镜像名中解析出来的
			if dgst == "" {
				// this is the only point at which we trust the registry. we use the
				// content headers to assemble a descriptor for the name. when this becomes
				// more robust, we mostly get this information from a secure trust store.
				// 从请求头中获取到摘要
				dgstHeader := digest.Digest(resp.Header.Get("Docker-Content-Digest"))

				if dgstHeader != "" && size != -1 {
					if err := dgstHeader.Validate(); err != nil {
						return "", ocispec.Descriptor{}, fmt.Errorf("%q in header not a valid digest: %w", dgstHeader, err)
					}
					dgst = dgstHeader
				}
			}
			// 如果没有正确获取到镜像摘要，那么使用GET方法获取menifest
			if dgst == "" || size == -1 {
				log.G(ctx).Debug("no Docker-Content-Digest header, fetching manifest instead")

				req = base.request(host, http.MethodGet, u...)
				if err := req.addNamespace(base.refspec.Hostname()); err != nil {
					return "", ocispec.Descriptor{}, err
				}

				for key, value := range r.resolveHeader {
					req.header[key] = append(req.header[key], value...)
				}

				resp, err := req.doWithRetries(ctx, nil)
				if err != nil {
					return "", ocispec.Descriptor{}, err
				}

				bodyReader := countingReader{reader: resp.Body}

				contentType = getManifestMediaType(resp)
				err = func() error {
					defer resp.Body.Close()
					if dgst != "" {
						_, err = io.Copy(io.Discard, &bodyReader)
						return err
					}

					if contentType == images.MediaTypeDockerSchema1Manifest {
						b, err := schema1.ReadStripSignature(&bodyReader)
						if err != nil {
							return err
						}

						dgst = digest.FromBytes(b)
						return nil
					}

					dgst, err = digest.FromReader(&bodyReader)
					return err
				}()
				if err != nil {
					return "", ocispec.Descriptor{}, err
				}
				size = bodyReader.bytesRead
			}
			// Prevent resolving to excessively large manifests
			if size > MaxManifestSize {
				if firstErr == nil {
					firstErr = fmt.Errorf("rejecting %d byte manifest for %s: %w", size, ref, errdefs.ErrNotFound)
				}
				continue
			}

			desc := ocispec.Descriptor{
				Digest:    dgst,
				MediaType: contentType,
				Size:      size,
			}

			log.G(ctx).WithField("desc.digest", desc.Digest).Debug("resolved")
			return ref, desc, nil
		}
	}

	// If above loop terminates without return, then there was an error.
	// "firstErr" contains the first non-404 error. That is, "firstErr == nil"
	// means that either no registries were given or each registry returned 404.

	if firstErr == nil {
		firstErr = fmt.Errorf("%s: %w", ref, errdefs.ErrNotFound)
	}

	return "", ocispec.Descriptor{}, firstErr
}

func (r *dockerResolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	// 通过传入的镜像名，解析镜像，获取可以下载当前镜像的镜像仓库的客户端配置
	base, err := r.resolveDockerBase(ref)
	if err != nil {
		return nil, err
	}

	return dockerFetcher{
		dockerBase: base,
	}, nil
}

func (r *dockerResolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	base, err := r.resolveDockerBase(ref)
	if err != nil {
		return nil, err
	}

	return dockerPusher{
		dockerBase: base,
		object:     base.refspec.Object,
		tracker:    r.tracker,
	}, nil
}

// 通过传入的镜像名，解析镜像，获取可以下载当前镜像的镜像仓库的客户端配置
func (r *dockerResolver) resolveDockerBase(ref string) (*dockerBase, error) {
	// 解析镜像名
	refspec, err := reference.Parse(ref)
	if err != nil {
		return nil, err
	}

	return r.base(refspec)
}

type dockerBase struct {
	// 当前要下载的镜像，被解析为了两个部分，Locator为取出tag以及摘要的前半段，而Object则为tag或者摘要
	refspec reference.Spec
	// 仓库位置，为reference.Spec.Locator去除host的剩下部分
	repository string
	// 下载当前镜像可以使用的镜像仓库
	hosts []RegistryHost
	// header为在/etc/containerd/certs.d/<host>/config.toml文件当中配置的请求头
	header http.Header
}

// 获取当前镜像的镜像仓库配置
func (r *dockerResolver) base(refspec reference.Spec) (*dockerBase, error) {
	// 解析镜像所使用的镜像仓库的域名
	host := refspec.Hostname()
	// 找到当前域名的镜像配置
	hosts, err := r.hosts(host)
	if err != nil {
		return nil, err
	}
	return &dockerBase{
		refspec:    refspec,
		repository: strings.TrimPrefix(refspec.Locator, host+"/"),
		hosts:      hosts,
		header:     r.header,
	}, nil
}

func (r *dockerBase) filterHosts(caps HostCapabilities) (hosts []RegistryHost) {
	for _, host := range r.hosts {
		if host.Capabilities.Has(caps) {
			hosts = append(hosts, host)
		}
	}
	return
}

func (r *dockerBase) request(host RegistryHost, method string, ps ...string) *request {
	header := r.header.Clone()
	if header == nil {
		header = http.Header{}
	}

	for key, value := range host.Header {
		header[key] = append(header[key], value...)
	}
	parts := append([]string{"/", host.Path, r.repository}, ps...)
	p := path.Join(parts...)
	// Join strips trailing slash, re-add ending "/" if included
	if len(parts) > 0 && strings.HasSuffix(parts[len(parts)-1], "/") {
		p = p + "/"
	}
	return &request{
		method: method,
		path:   p,
		header: header,
		host:   host,
	}
}

func (r *request) authorize(ctx context.Context, req *http.Request) error {
	// Check if has header for host
	if r.host.Authorizer != nil {
		if err := r.host.Authorizer.Authorize(ctx, req); err != nil {
			return err
		}
	}

	return nil
}

func (r *request) addNamespace(ns string) (err error) {
	if !r.host.isProxy(ns) {
		return nil
	}
	var q url.Values
	// Parse query
	if i := strings.IndexByte(r.path, '?'); i > 0 {
		r.path = r.path[:i+1]
		q, err = url.ParseQuery(r.path[i+1:])
		if err != nil {
			return
		}
	} else {
		r.path = r.path + "?"
		q = url.Values{}
	}
	q.Add("ns", ns)

	r.path = r.path + q.Encode()

	return
}

type request struct {
	method string
	path   string
	header http.Header
	host   RegistryHost
	body   func() (io.ReadCloser, error)
	size   int64
}

func (r *request) do(ctx context.Context) (*http.Response, error) {
	u := r.host.Scheme + "://" + r.host.Host + r.path
	req, err := http.NewRequestWithContext(ctx, r.method, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header = http.Header{} // headers need to be copied to avoid concurrent map access
	for k, v := range r.header {
		req.Header[k] = v
	}
	if r.body != nil {
		body, err := r.body()
		if err != nil {
			return nil, err
		}
		req.Body = body
		req.GetBody = r.body
		if r.size > 0 {
			req.ContentLength = r.size
		}
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("url", u))
	log.G(ctx).WithFields(requestFields(req)).Debug("do request")
	if err := r.authorize(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to authorize: %w", err)
	}

	client := &http.Client{}
	if r.host.Client != nil {
		*client = *r.host.Client
	}
	if client.CheckRedirect == nil {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			if err := r.authorize(ctx, req); err != nil {
				return fmt.Errorf("failed to authorize redirect: %w", err)
			}
			return nil
		}
	}
	_, httpSpan := tracing.StartSpan(
		ctx,
		tracing.Name("remotes.docker.resolver", "HTTPRequest"),
		tracing.WithHTTPRequest(req),
	)
	defer httpSpan.End()
	// 发出请求
	resp, err := client.Do(req)
	if err != nil {
		httpSpan.SetStatus(err)
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	httpSpan.SetAttributes(tracing.HTTPStatusCodeAttributes(resp.StatusCode)...)
	log.G(ctx).WithFields(responseFields(resp)).Debug("fetch response received")
	return resp, nil
}

// 解析请求，如果失败，尝试重新下载
func (r *request) doWithRetries(ctx context.Context, responses []*http.Response) (*http.Response, error) {
	resp, err := r.do(ctx)
	if err != nil {
		return nil, err
	}

	responses = append(responses, resp)
	retry, err := r.retryRequest(ctx, responses)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	if retry {
		resp.Body.Close()
		return r.doWithRetries(ctx, responses)
	}
	return resp, err
}

func (r *request) retryRequest(ctx context.Context, responses []*http.Response) (bool, error) {
	if len(responses) > 5 {
		return false, nil
	}
	last := responses[len(responses)-1]
	switch last.StatusCode {
	case http.StatusUnauthorized:
		log.G(ctx).WithField("header", last.Header.Get("WWW-Authenticate")).Debug("Unauthorized")
		if r.host.Authorizer != nil {
			if err := r.host.Authorizer.AddResponses(ctx, responses); err == nil {
				return true, nil
			} else if !errdefs.IsNotImplemented(err) {
				return false, err
			}
		}

		return false, nil
	case http.StatusMethodNotAllowed:
		// Support registries which have not properly implemented the HEAD method for
		// manifests endpoint
		if r.method == http.MethodHead && strings.Contains(r.path, "/manifests/") {
			r.method = http.MethodGet
			return true, nil
		}
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true, nil
	}

	// TODO: Handle 50x errors accounting for attempt history
	return false, nil
}

func (r *request) String() string {
	return r.host.Scheme + "://" + r.host.Host + r.path
}

func requestFields(req *http.Request) log.Fields {
	fields := map[string]interface{}{
		"request.method": req.Method,
	}
	for k, vals := range req.Header {
		k = strings.ToLower(k)
		if k == "authorization" {
			continue
		}
		for i, v := range vals {
			field := "request.header." + k
			if i > 0 {
				field = fmt.Sprintf("%s.%d", field, i)
			}
			fields[field] = v
		}
	}

	return log.Fields(fields)
}

func responseFields(resp *http.Response) log.Fields {
	fields := map[string]interface{}{
		"response.status": resp.Status,
	}
	for k, vals := range resp.Header {
		k = strings.ToLower(k)
		for i, v := range vals {
			field := "response.header." + k
			if i > 0 {
				field = fmt.Sprintf("%s.%d", field, i)
			}
			fields[field] = v
		}
	}

	return log.Fields(fields)
}

// IsLocalhost checks if the registry host is local.
func IsLocalhost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	return ip.IsLoopback()
}
