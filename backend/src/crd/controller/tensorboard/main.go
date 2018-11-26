package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	kubeflowGroupClientSet "github.com/kubeflow/pipelines/backend/src/crd/pkg/client/clientset/versioned"
	informers "github.com/kubeflow/pipelines/backend/src/crd/pkg/client/informers/externalversions"
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

type Controller struct {
	kc  *kubernetes.Clientset
	cs  *kubeflowGroupClientSet.Clientset
	inf informers.SharedInformerFactory
}

func NewController(kc *kubernetes.Clientset, cs *kubeflowGroupClientSet.Clientset, inf informers.SharedInformerFactory) *Controller {
	return &Controller{}
}

func main() {
	flag.Parse()

	fmt.Println("Tensorboard controller starting up...")

	// stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		log.Fatalf("Failed to build a valid Kubernetes client config from the supplied flags: %v", err)
	}

	kubeClient := kubernetes.NewForConfigOrDie(cfg)
	clientSet := kubeflowGroupClientSet.NewForConfigOrDie(cfg)
	informersFactory := informers.NewSharedInformerFactory(clientSet, 30*time.Second)

	_ = NewController(kubeClient, clientSet, informersFactory)
}
