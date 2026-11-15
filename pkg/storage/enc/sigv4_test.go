package enc

import (
	"encoding/hex"
	"net/url"
	"testing"
	"time"
)

const awsExampleSecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"

// The signing-key derivation example of the AWS General Reference
// ("Deriving a signing key").
func TestSigV4SigningKeyPublishedExample(t *testing.T) {
	got := hex.EncodeToString(sigV4SigningKey(awsExampleSecret, "20120215", "us-east-1", "iam"))
	if want := "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"; got != want {
		t.Fatalf("signing key %s, want %s", got, want)
	}
}

// The worked example of the AWS General Reference's "Signature Version 4
// signing process": IAM ListUsers on 30 August 2015, each stage as the
// documentation prints it.
func TestSigV4PublishedWorkedExample(t *testing.T) {
	out := sigV4Sign(sigV4Input{
		Method: "GET", Path: "/",
		Query: url.Values{"Action": {"ListUsers"}, "Version": {"2010-05-08"}},
		Headers: map[string]string{
			"Host":         "iam.amazonaws.com",
			"Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
			"X-Amz-Date":   "20150830T123600Z",
		},
		AccessKey: "AKIDEXAMPLE", SecretKey: awsExampleSecret,
		Region: "us-east-1", Service: "iam",
		Time: time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC),
	})
	wantCanonical := "GET\n/\nAction=ListUsers&Version=2010-05-08\n" +
		"content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n\n" +
		"content-type;host;x-amz-date\n" +
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if out.CanonicalRequest != wantCanonical {
		t.Fatalf("canonical request:\n%s\nwant:\n%s", out.CanonicalRequest, wantCanonical)
	}
	wantToSign := "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n" +
		"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
	if out.StringToSign != wantToSign {
		t.Fatalf("string to sign:\n%s\nwant:\n%s", out.StringToSign, wantToSign)
	}
	if want := "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"; out.Signature != want {
		t.Fatalf("signature %s, want %s", out.Signature, want)
	}
	wantAuth := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, " +
		"SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	if out.Authorization != wantAuth {
		t.Fatalf("authorization %q, want %q", out.Authorization, wantAuth)
	}
}

// Two cases of AWS's Signature Version 4 test suite (aws-sig-v4-test-
// suite): get-vanilla and post-vanilla, against the service named
// "service".
func TestSigV4TestSuiteVanilla(t *testing.T) {
	for _, c := range []struct{ method, want string }{
		{"GET", "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"},
		{"POST", "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"},
	} {
		out := sigV4Sign(sigV4Input{
			Method: c.method, Path: "/",
			Headers:   map[string]string{"Host": "example.amazonaws.com", "X-Amz-Date": "20150830T123600Z"},
			AccessKey: "AKIDEXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
			Region: "us-east-1", Service: "service",
			Time: time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC),
		})
		if out.Signature != c.want {
			t.Errorf("%s vanilla: signature %s, want %s", c.method, out.Signature, c.want)
		}
	}
}
