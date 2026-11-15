package enc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The awskms root: AWS Key Management Service.
//
//	awskms:<key arn>[?options]
//	awskms:arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab
//
// The region is the ARN's. The calls are the JSON 1.1 protocol's
// TrentService.Encrypt and TrentService.Decrypt, signed with Signature
// Version 4 (sigv4.go). The binding is the encryption context
// {"datax-cluster": ..., "datax-node": ...}, which KMS authenticates,
// records in CloudTrail and lets a key policy condition on.
//
// Credentials are looked for where the AWS tools look, in this order:
//
//  1. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN)
//  2. AWS_WEB_IDENTITY_TOKEN_FILE with AWS_ROLE_ARN: the role is assumed
//     through STS (EKS service accounts)
//  3. the shared credentials file (AWS_SHARED_CREDENTIALS_FILE, or
//     ~/.aws/credentials), profile AWS_PROFILE or "default"
//  4. the container credentials endpoint (ECS, EKS Pod Identity):
//     AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or _FULL_URI
//  5. the instance's role, from the instance metadata service (IMDSv2)
//
// Options:
//
//	profile            the profile of the shared credentials file
//	credentials_file   the shared credentials file
//	endpoint           the KMS endpoint, for a VPC or FIPS endpoint
//	sts_endpoint       the STS endpoint, for web identity
//	imds               the instance metadata endpoint
//	                   (AWS_EC2_METADATA_SERVICE_ENDPOINT)
//	ca, timeout, retries
type awsService struct {
	h        *kmsHTTP
	arn      string
	region   string
	endpoint string
	host     string

	profile, credsFile string
	stsEndpoint        string
	imds               string

	mu     sync.Mutex
	cached awsCredentials
}

type awsCredentials struct {
	accessKey, secretKey, sessionToken string
	// expires is zero for credentials that do not expire.
	expires time.Time
}

func newAWSKMSRoot(rest string) (Root, error) {
	const kind = "awskms"
	arn, opts, err := splitKMSSpec(kind, rest)
	if err != nil {
		return nil, err
	}
	// arn:<partition>:kms:<region>:<account>:key/<id> (or alias/<name>).
	p := strings.SplitN(arn, ":", 6)
	if len(p) != 6 || p[0] != "arn" || p[2] != "kms" || p[3] == "" || p[4] == "" ||
		!(strings.HasPrefix(p[5], "key/") || strings.HasPrefix(p[5], "alias/")) {
		return nil, fmt.Errorf("awskms root: %q is not a KMS key ARN (want awskms:arn:aws:kms:<region>:<account>:key/<id>)", arn)
	}
	s := &awsService{arn: arn, region: p[3]}
	suffix := "amazonaws.com"
	if p[1] == "aws-cn" {
		suffix = "amazonaws.com.cn"
	}
	s.endpoint = "https://kms." + s.region + "." + suffix
	if v := opts.take("endpoint"); v != "" {
		if s.endpoint, err = endpointURL(kind, "endpoint", v); err != nil {
			return nil, err
		}
	}
	u, _ := url.Parse(s.endpoint)
	s.host = u.Host
	s.stsEndpoint = "https://sts." + s.region + "." + suffix
	if v := opts.take("sts_endpoint"); v != "" {
		if s.stsEndpoint, err = endpointURL(kind, "sts_endpoint", v); err != nil {
			return nil, err
		}
	}
	s.imds = "http://169.254.169.254"
	if v := opts.takeOr("imds", "AWS_EC2_METADATA_SERVICE_ENDPOINT"); v != "" {
		if s.imds, err = metadataURL(kind, "imds", v); err != nil {
			return nil, err
		}
	}
	s.profile = opts.takeOr("profile", "AWS_PROFILE")
	if s.profile == "" {
		s.profile = "default"
	}
	s.credsFile = opts.takeOr("credentials_file", "AWS_SHARED_CREDENTIALS_FILE")
	if s.h, err = newKMSHTTP(opts, "AWS KMS key "+arn, "AWS_CA_BUNDLE"); err != nil {
		return nil, err
	}
	if err := opts.done(); err != nil {
		return nil, err
	}
	return &kmsRoot{kind: kind, spec: rest, key: arn, svc: s}, nil
}

// metadataURL validates a metadata-service override. Unlike a service
// endpoint it may be plain HTTP to another address: the metadata
// services are link-local and speak nothing else.
func metadataURL(kind, option, value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("%s root: %s %q is not an http(s) URL", kind, option, value)
	}
	return strings.TrimRight(u.Scheme+"://"+u.Host+u.Path, "/"), nil
}

// credentials returns credentials for one request. Those that expire are
// cached until shortly before they do; those that do not are read again
// each time, since reading them is cheap and the operator may have
// replaced them.
func (s *awsService) credentials(ctx context.Context) (awsCredentials, error) {
	if id, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"); id != "" && secret != "" {
		return awsCredentials{accessKey: id, secretKey: secret, sessionToken: os.Getenv("AWS_SESSION_TOKEN")}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached.accessKey != "" && time.Until(s.cached.expires) > kmsTokenMargin {
		return s.cached, nil
	}
	if tokenFile, role := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), os.Getenv("AWS_ROLE_ARN"); tokenFile != "" && role != "" {
		c, err := s.webIdentity(ctx, tokenFile, role)
		if err != nil {
			return awsCredentials{}, err
		}
		s.cached = c
		return c, nil
	}
	if c, ok, err := s.sharedFile(); err != nil {
		return awsCredentials{}, err
	} else if ok {
		return c, nil
	}
	if rel, full := os.Getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"), os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"); rel != "" || full != "" {
		c, err := s.container(ctx, rel, full)
		if err != nil {
			return awsCredentials{}, err
		}
		s.cached = c
		return c, nil
	}
	if strings.EqualFold(os.Getenv("AWS_EC2_METADATA_DISABLED"), "true") {
		return awsCredentials{}, &kmsError{msg: "no AWS credentials: none in the environment or the shared credentials file, and the instance metadata service is disabled (AWS_EC2_METADATA_DISABLED)"}
	}
	c, err := s.instanceRole(ctx)
	if err != nil {
		return awsCredentials{}, &kmsError{msg: fmt.Sprintf("no AWS credentials: none in the environment or the shared credentials file, and the instance role could not be read (%v)", err), transient: kmsTransient(err)}
	}
	s.cached = c
	return c, nil
}

// sharedFile reads one profile of the shared credentials file. A file
// that is absent, or has no such profile, is not an error: the next
// source is tried.
func (s *awsService) sharedFile() (awsCredentials, bool, error) {
	path := s.credsFile
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return awsCredentials{}, false, nil
		}
		path = filepath.Join(home, ".aws", "credentials")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if s.credsFile != "" {
			return awsCredentials{}, false, &kmsError{msg: fmt.Sprintf("reading the AWS credentials file: %v", err)}
		}
		return awsCredentials{}, false, nil
	}
	defer Zero(raw)
	var c awsCredentials
	in := false
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' && line[len(line)-1] == ']' {
			in = strings.TrimSpace(line[1:len(line)-1]) == s.profile
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !in || !ok {
			continue
		}
		switch value = strings.TrimSpace(value); strings.ToLower(strings.TrimSpace(name)) {
		case "aws_access_key_id":
			c.accessKey = value
		case "aws_secret_access_key":
			c.secretKey = value
		case "aws_session_token":
			c.sessionToken = value
		}
	}
	return c, c.accessKey != "" && c.secretKey != "", nil
}

// awsCredentialDoc is the shape the instance metadata service and the
// container endpoint both answer in.
type awsCredentialDoc struct {
	Code            string
	Message         string
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string
	Token           string
	Expiration      time.Time
}

func (s *awsService) fromDoc(raw []byte, source string) (awsCredentials, error) {
	defer Zero(raw)
	var d awsCredentialDoc
	if err := json.Unmarshal(raw, &d); err != nil || d.AccessKeyID == "" || d.SecretAccessKey == "" {
		why := "it is not a credentials document"
		if d.Code != "" && d.Code != "Success" {
			why = d.Code + ": " + d.Message
		}
		return awsCredentials{}, &kmsError{msg: fmt.Sprintf("%s answered without credentials: %s", source, why)}
	}
	return awsCredentials{accessKey: d.AccessKeyID, secretKey: d.SecretAccessKey, sessionToken: d.Token, expires: d.Expiration}, nil
}

// instanceRole reads the instance role's credentials from the metadata
// service, version 2 only: a session token is fetched with a PUT, which
// an open proxy or a server-side request forgery cannot forge, and the
// unauthenticated version 1 is never fallen back to.
func (s *awsService) instanceRole(ctx context.Context) (awsCredentials, error) {
	get := func(op, method, path string, header map[string]string) ([]byte, error) {
		return s.h.once(ctx, kmsRequest{op: op, local: true, build: func(ctx context.Context) (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, method, s.imds+path, nil)
			if err != nil {
				return nil, err
			}
			for k, v := range header {
				req.Header.Set(k, v)
			}
			return req, nil
		}})
	}
	raw, err := get("opening a metadata session", http.MethodPut, "/latest/api/token", map[string]string{"X-aws-ec2-metadata-token-ttl-seconds": "300"})
	if err != nil {
		return awsCredentials{}, err
	}
	session := map[string]string{"X-aws-ec2-metadata-token": s.h.secret(strings.TrimSpace(string(raw)))}
	raw, err = get("naming the instance role", http.MethodGet, "/latest/meta-data/iam/security-credentials/", session)
	if err != nil {
		return awsCredentials{}, err
	}
	role, _, _ := strings.Cut(strings.TrimSpace(string(raw)), "\n")
	if role == "" {
		return awsCredentials{}, &kmsError{msg: "the instance has no IAM role"}
	}
	raw, err = get("reading the instance role's credentials", http.MethodGet, "/latest/meta-data/iam/security-credentials/"+url.PathEscape(role), session)
	if err != nil {
		return awsCredentials{}, err
	}
	return s.fromDoc(raw, "the instance metadata service")
}

// container reads credentials from the endpoint a container platform
// provides its tasks.
func (s *awsService) container(ctx context.Context, rel, full string) (awsCredentials, error) {
	target := full
	if rel != "" {
		target = "http://169.254.170.2" + rel
	}
	auth := os.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN")
	if f := os.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"); f != "" {
		t, err := readSecretFile(f)
		if err != nil {
			return awsCredentials{}, &kmsError{msg: fmt.Sprintf("reading the container authorization token: %v", err)}
		}
		auth = t
	}
	raw, err := s.h.once(ctx, kmsRequest{op: "reading the container's credentials", local: true, build: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, &kmsError{msg: "the container credentials URI is not a URL"}
		}
		if auth != "" {
			req.Header.Set("Authorization", s.h.secret(auth))
		}
		return req, nil
	}})
	if err != nil {
		return awsCredentials{}, &kmsError{msg: fmt.Sprintf("the container credentials endpoint: %v", err), transient: kmsTransient(err)}
	}
	return s.fromDoc(raw, "the container credentials endpoint")
}

// webIdentity exchanges the token an orchestrator projects into the pod
// for a role's credentials. The call to STS is the one AWS request that
// is not signed: the token is the proof.
func (s *awsService) webIdentity(ctx context.Context, tokenFile, role string) (awsCredentials, error) {
	token, err := readSecretFile(tokenFile)
	if err != nil {
		return awsCredentials{}, &kmsError{msg: fmt.Sprintf("reading the web identity token: %v", err)}
	}
	session := os.Getenv("AWS_ROLE_SESSION_NAME")
	if session == "" {
		session = "datax"
	}
	form := url.Values{
		"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"},
		"RoleArn": {role}, "RoleSessionName": {session}, "WebIdentityToken": {s.h.secret(token)},
	}.Encode()
	raw, err := s.h.once(ctx, kmsRequest{op: "assuming the role with a web identity", build: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.stsEndpoint+"/", strings.NewReader(form))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req, nil
	}, reason: func(_ int, body []byte) (string, bool) {
		var e struct {
			Error struct{ Code, Message string }
		}
		if xml.Unmarshal(body, &e) != nil || e.Error.Code == "" {
			return "", false
		}
		return e.Error.Code + ": " + e.Error.Message, false
	}})
	if err != nil {
		return awsCredentials{}, &kmsError{msg: fmt.Sprintf("STS AssumeRoleWithWebIdentity for %s: %v", role, err), transient: kmsTransient(err)}
	}
	defer Zero(raw)
	var resp struct {
		Result struct {
			Credentials struct {
				AccessKeyID     string `xml:"AccessKeyId"`
				SecretAccessKey string
				SessionToken    string
				Expiration      time.Time
			}
		} `xml:"AssumeRoleWithWebIdentityResult"`
	}
	c := &resp.Result.Credentials
	if err := xml.Unmarshal(raw, &resp); err != nil || c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return awsCredentials{}, &kmsError{msg: "STS answered AssumeRoleWithWebIdentity without credentials"}
	}
	return awsCredentials{accessKey: c.AccessKeyID, secretKey: c.SecretAccessKey, sessionToken: c.SessionToken, expires: c.Expiration}, nil
}

// awsReason is KMS's error body, {"__type": "...", "message": "..."}. A
// throttled request is refused with status 400, and passes.
func awsReason(_ int, body []byte) (string, bool) {
	var e struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
		Upper   string `json:"Message"`
	}
	if json.Unmarshal(body, &e) != nil || e.Type == "" {
		return "", false
	}
	// The type may arrive qualified: "com.amazonaws.kms#Name".
	if i := strings.LastIndexByte(e.Type, '#'); i >= 0 {
		e.Type = e.Type[i+1:]
	}
	if e.Message == "" {
		e.Message = e.Upper
	}
	transient := false
	switch e.Type {
	case "ThrottlingException", "LimitExceededException", "RequestLimitExceeded", "KMSInternalException", "DependencyTimeoutException", "KeyUnavailableException":
		transient = true
	}
	return strings.TrimSpace(e.Type + ": " + e.Message), transient
}

func (s *awsService) context(b Binding) map[string]string {
	if b.IsZero() {
		return nil
	}
	return map[string]string{"datax-cluster": b.ClusterID, "datax-node": strconv.FormatUint(b.NodeID, 10)}
}

func (s *awsService) call(ctx context.Context, op, target string, body map[string]any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	return s.h.do(ctx, kmsRequest{op: op, reason: awsReason, build: func(ctx context.Context) (*http.Request, error) {
		creds, err := s.credentials(ctx)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		headers := map[string]string{
			"Host":         s.host,
			"Content-Type": "application/x-amz-json-1.1",
			"X-Amz-Date":   now.Format("20060102T150405Z"),
			"X-Amz-Target": "TrentService." + target,
		}
		if creds.sessionToken != "" {
			headers["X-Amz-Security-Token"] = s.h.secret(creds.sessionToken)
		}
		s.h.secret(creds.secretKey)
		sig := sigV4Sign(sigV4Input{
			Method: http.MethodPost, Path: "/", Headers: headers, Payload: raw,
			AccessKey: creds.accessKey, SecretKey: creds.secretKey,
			Region: s.region, Service: "kms", Time: now,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+"/", bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			if k != "Host" {
				req.Header.Set(k, v)
			}
		}
		// The signature is not a credential that outlives the request,
		// but it is struck from messages all the same.
		s.h.secret(sig.Signature)
		req.Header.Set("Authorization", sig.Authorization)
		return req, nil
	}})
}

func (s *awsService) encrypt(ctx context.Context, plaintext []byte, b Binding) ([]byte, string, error) {
	body := map[string]any{"KeyId": s.arn, "Plaintext": base64.StdEncoding.EncodeToString(plaintext)}
	if c := s.context(b); c != nil {
		body["EncryptionContext"] = c
	}
	raw, err := s.call(ctx, "Encrypt", "Encrypt", body)
	if err != nil {
		return nil, "", err
	}
	var resp struct{ CiphertextBlob string }
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, "", fmt.Errorf("%s: Encrypt answered with something that is not JSON", s.h.what)
	}
	out, err := base64.StdEncoding.DecodeString(resp.CiphertextBlob)
	if err != nil || len(out) == 0 {
		return nil, "", fmt.Errorf("%s: Encrypt answered without a ciphertext", s.h.what)
	}
	// KMS rotates a key's material behind one key id and names no
	// version: the ciphertext itself says which material made it.
	return out, "", nil
}

func (s *awsService) decrypt(ctx context.Context, ciphertext []byte, _ string, b Binding) ([]byte, error) {
	// The key is named, though KMS could find it from the ciphertext, so
	// that a blob made under some other key the credential can use is
	// refused rather than decrypted.
	body := map[string]any{"KeyId": s.arn, "CiphertextBlob": base64.StdEncoding.EncodeToString(ciphertext)}
	if c := s.context(b); c != nil {
		body["EncryptionContext"] = c
	}
	raw, err := s.call(ctx, "Decrypt", "Decrypt", body)
	if err != nil {
		return nil, err
	}
	defer Zero(raw)
	var resp struct{ Plaintext string }
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("%s: Decrypt answered with something that is not JSON", s.h.what)
	}
	out, err := base64.StdEncoding.DecodeString(resp.Plaintext)
	if err != nil || len(out) == 0 {
		return nil, fmt.Errorf("%s: Decrypt answered without a plaintext", s.h.what)
	}
	return out, nil
}
