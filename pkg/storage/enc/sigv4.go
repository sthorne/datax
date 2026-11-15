package enc

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"time"
)

// AWS Signature Version 4, as much of it as a JSON-protocol service
// needs: a request with a known body, signed in its Authorization
// header. Each step is returned as well as the result, because the steps
// are what AWS publishes worked examples of and what the tests hold this
// to (sigv4_test.go).

// sigV4Input is a request to sign.
type sigV4Input struct {
	Method string
	// Path is the URI path, already as it goes on the wire.
	Path string
	// Query is the decoded query parameters.
	Query url.Values
	// Headers are the headers to sign. Host must be among them.
	Headers map[string]string
	Payload []byte

	AccessKey, SecretKey string
	Region, Service      string
	Time                 time.Time
}

// sigV4Output is each stage of the signature.
type sigV4Output struct {
	CanonicalRequest string
	StringToSign     string
	Signature        string
	// Authorization is the header value.
	Authorization string
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// sigV4SigningKey derives the key a day's requests to one service in one
// region are signed with.
func sigV4SigningKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}

// sigV4Escape percent-encodes as SigV4 wants: everything but the
// unreserved characters, with upper-case hex, and a space as %20.
func sigV4Escape(s string) string {
	const hexUpper = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexUpper[c>>4])
		b.WriteByte(hexUpper[c&15])
	}
	return b.String()
}

func sigV4Sign(in sigV4Input) sigV4Output {
	amzDate := in.Time.UTC().Format("20060102T150405Z")
	date := amzDate[:8]

	// The query: each name and value encoded, sorted by name then value.
	var pairs []string
	for name, vals := range in.Query {
		for _, v := range vals {
			pairs = append(pairs, sigV4Escape(name)+"="+sigV4Escape(v))
		}
	}
	sort.Strings(pairs)

	// The headers: names lower-cased and sorted, values trimmed with
	// runs of spaces collapsed.
	names := make([]string, 0, len(in.Headers))
	lower := make(map[string]string, len(in.Headers))
	for name, v := range in.Headers {
		n := strings.ToLower(name)
		names = append(names, n)
		lower[n] = strings.Join(strings.Fields(v), " ")
	}
	sort.Strings(names)
	var headers strings.Builder
	for _, n := range names {
		headers.WriteString(n + ":" + lower[n] + "\n")
	}
	signed := strings.Join(names, ";")

	path := in.Path
	if path == "" {
		path = "/"
	}
	payloadHash := sha256.Sum256(in.Payload)
	canonical := strings.Join([]string{
		in.Method,
		path,
		strings.Join(pairs, "&"),
		headers.String(),
		signed,
		hex.EncodeToString(payloadHash[:]),
	}, "\n")

	scope := date + "/" + in.Region + "/" + in.Service + "/aws4_request"
	canonicalHash := sha256.Sum256([]byte(canonical))
	toSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(canonicalHash[:])}, "\n")
	signature := hex.EncodeToString(hmacSHA256(sigV4SigningKey(in.SecretKey, date, in.Region, in.Service), toSign))
	return sigV4Output{
		CanonicalRequest: canonical,
		StringToSign:     toSign,
		Signature:        signature,
		Authorization:    "AWS4-HMAC-SHA256 Credential=" + in.AccessKey + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + signature,
	}
}
