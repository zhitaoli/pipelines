package main

import (
	"flag"
	"fmt"
	"log"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	kubeflowGroupClientSet "github.com/kubeflow/pipelines/backend/src/crd/pkg/client/clientset/versioned"
	"github.com/kubeflow/pipelines/backend/src/crd/pkg/signals"
)

var (
	masterURL = flag.String(
		"master",
		"",
		"Address of the Kubernetes API server. Can be empty if running outside of a k8s cluster.",
	)

	kubeconfig = flag.String(
		"kubeconfig",
		"",
		"Path to kubeconfig. Can be empty if running outside of a k8s cluster.",
	)
)

func main() {
	flag.Parse()

	fmt.Println("Tensorboard controller starting up...")

	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		log.Fatalf("Failed to build a valid Kubernetes client config from the supplied flags: %v", err)
	}

	kubeClient := kubernetes.NewForConfigOrDie(cfg)

	tbClient := kubeflowGroupClientSet.NewForConfigOrDie(cfg).Te
}
