package main

import (
	"bytes"
	"io"
	"strings"

	"github.com/golang/glog"
	kapiv1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

var option = &kapiv1.PodExecOptions{
	Command:   []string{"redis-cli", "info"},
	Container: "redis-node",
	Stdin:     false,
	Stdout:    true,
	Stderr:    false,
	TTY:       false,
}

func execCommandOnPod(restConfig *rest.Config, clientset *kubernetes.Clientset, pod *kapiv1.Pod, parameterCodec apiruntime.ParameterCodec, commandOverride []string) (bytes.Buffer, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec")
	tmpOption := option
	if len(commandOverride) > 0 {
		tmpOption.Command = commandOverride
	}
	req.VersionedParams(tmpOption, parameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		glog.Fatalf("exec to pod %s failed while retrieving `info`", pod.Name)
	}
	var stdout bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: &stdout,
		Tty:    false,
	})
	return stdout, err
}

func parseCommandOutput(stdout bytes.Buffer, pod *kapiv1.Pod) []string {
	var lines []string
	for {
		line, err := stdout.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			glog.Fatalf("failed to ReadString from byte buffer while processing node info output from pod %s", pod.Name)
			break
		}
		lines = append(lines, strings.Trim(line, "\r\n"))
	}
	return lines
}

func getInfoValueString(line string) string {
	lineArray := strings.Split(line, ":")
	if len(lineArray) > 1 {
		return strings.Trim(lineArray[1], "\r\n")
	}
	return ""
}
