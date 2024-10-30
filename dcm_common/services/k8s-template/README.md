# Template for service-deployment with kubernetes
This serves as a brief description of how to deploy a dcm-service using kubernetes/minikube.

## Preparation
* get [`minikube`](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download) and `kubectl`
* build queue-registry-app-image
* build dcm-service-app-image
* modify dcm-service-deployment.yaml
  * (optional) update reference to queue-registry-app-image
  * (optional) update reference to dcm-service-app-image
  * (optional) update volume mount paths
  * (optional) update environment variables

## Run
* run minikube-cluster
  `minikube start`
* make required images available to minikube
  `minikube load <image>`

  use together with `list` and `rm` to replace existing images; local images are only used if `imagePullPolicy: Never` is set in manifest
* mount volume in cluster, e.g.,
  `minikube mount file_storage/:/file_storage`
* activate tunnel (required to use loadBalancer-type-services)
  `minikube tunnel`
* apply deployment
  `kubectl apply -f dcm-service-deployment.yaml`
* to access deployments via swagger-ui, forward ports of cluster
  `kubectl port-forward deployment/dcm-service-deployment 8080:8080`
* delete resources
  `kubectl delete ...`
* afterwards: shut down minikube
  `minikube stop`
