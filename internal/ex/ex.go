package ex

import (
        "log"
        "os/exec"
        "bytes"
)

type Exec struct {
     Name string
}

func NewExec() *Exec{
        return &Exec{
             Name: "exec",
        }
}

func (e *Exec) Execmd(cmdstr string) (string, error) {
        cmd := exec.Command("/bin/bash", "-c", cmdstr)
        var out bytes.Buffer
        cmd.Stdout = &out
        err := cmd.Run()
        if err != nil {
                log.Printf("error: %s", err)
                return "", err
        }
        return out.String(), err
}

