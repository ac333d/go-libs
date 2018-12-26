package nethttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"time"
)

// GetBytes - GetBytes
func GetBytes(url string, headers, params map[string]string) ([]byte, int, error) {

	client := &http.Client{
		Timeout: time.Duration(5 * time.Second),
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte{}, -1, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	} else if err != nil {
		return nil, -1, err
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode > 210 {
		return nil, resp.StatusCode, fmt.Errorf("%s", string(bodyBytes))
	}

	return bodyBytes, resp.StatusCode, nil
}

// Get - Get
func Get(url string, header, params map[string]string) (interface{}, int, error) {
	var response interface{}

	body, code, err := GetBytes(url, header, params)
	if err != nil {
		return nil, code, err
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, code, err
	}
	return response, code, nil
}

// PostJSON - PostJSON
func PostJSON(url string, header, params map[string]string, request interface{}, response interface{}) (int, error) {

	body, err := json.Marshal(request)
	if err != nil {
		return -1, err
	}

	client := &http.Client{
		Timeout: time.Duration(5 * time.Second),
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return -1, err
	}

	req.Header.Add("Content-Type", "application/json")
	for h, v := range header {
		req.Header.Add(h, v)
	}

	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	} else if err != nil {
		return -1, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}

	if resp.StatusCode > 210 {
		return resp.StatusCode, fmt.Errorf("%s", string(data))
	}

	if err := json.Unmarshal(data, response); err != nil {
		return resp.StatusCode, err
	}

	return resp.StatusCode, nil
}

// GetIPFromReq return client's real public IP address from http request headers.
func GetIPFromReq(r *http.Request) string {
	xTrueClientIP := r.Header.Get("True-Client-IP")
	if xTrueClientIP != "" {
		return xTrueClientIP
	}

	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		return xForwardedFor
	}

	xRealIP := r.Header.Get("X-Real-Ip")
	if xRealIP != "" {
		return xRealIP
	}

	if strings.ContainsRune(r.RemoteAddr, ':') {
		remoteIP, _, _ := net.SplitHostPort(r.RemoteAddr)
		return remoteIP
	}
	return r.RemoteAddr
}

// GetUserAgentFromReq return client's user agent string from http request headers.
func GetUserAgentFromReq(r *http.Request) string {
	u := r.Header.Get("User-Agent")
	if u != "" {
		return u
	}
	return "Unknown device"
}

// GetISPLocationFromReq return client's ISP location from http request headers.
// TODO: Complete this function
func GetISPLocationFromReq(r *http.Request) string {
	return "Unknown location"
}

// PostFormDataWithHeaders - Creates a new file upload http request with optional extra params along with headers
func PostFormDataWithHeaders(uri string, params map[string]string, headers map[string]string, paramName string, fileContents []byte, fileName string, res interface{}) (int, error) {
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(paramName, fileName)
	if err != nil {
		return -1, err
	}
	part.Write(fileContents)

	for key, val := range params {
		_ = writer.WriteField(key, val)
	}
	err = writer.Close()
	if err != nil {
		return -1, err
	}

	request, err := http.NewRequest("POST", uri, body)
	if err != nil {
		return -1, err
	}
	request.Header.Add("Content-Type", writer.FormDataContentType())
	for k, v := range headers {
		request.Header.Add(k, v)
	}

	client := &http.Client{
		Timeout: time.Duration(5 * time.Second),
	}

	resp, err := client.Do(request)

	if resp != nil {
		defer resp.Body.Close()
	} else if err != nil {
		return resp.StatusCode, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}

	if err := json.Unmarshal(data, &res); err != nil {
		return resp.StatusCode, err
	}
	return resp.StatusCode, nil
}
