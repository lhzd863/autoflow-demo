package util

import (
        "net"
        "time"
	"io/ioutil"
	"net/http"
	"strings"
)

//post
func Api_RequestPost(url string, para string) (string, error) {
	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(netw, addr, time.Second*60) //设置建立连接超时
				if err != nil {
					return nil, err
				}
				conn.SetDeadline(time.Now().Add(time.Second * 60)) //设置发送接受数据超时
				return conn, nil
			},
			ResponseHeaderTimeout: time.Second * 60,
		},
	}
	req, err := http.NewRequest("POST", url, strings.NewReader(para))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json;charset=utf-8")
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

//delete
func Api_RequestDelete(url string, para string) (string, error) {
	req, err := http.NewRequest("DELETE", url, strings.NewReader(para))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json;charset=utf-8")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

//get
func Api_RequestGet(url string, para string) (string, error) {
	req, err := http.NewRequest("GET", url, strings.NewReader(para))
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

//put
func Api_RequestPut(url string, para string) (string, error) {
	req, err := http.NewRequest("PUT", url, strings.NewReader(para))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json;charset=utf-8")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
