## Opencost Compare
This repository hosts a general set of models used to parse HTTP responses and a single test to provide a comparison between two versions of `opencost`.

### Requirements 

* The version of `opencost` that you wish to compare should exist in the `../opencost` directory relative to this repository.
* A "target" opencost version running in Kubernetes (and accessible `Service`) that can be used to compare against the local version.
* A Prometheus server that is being used by the target (and accessible `Service`).


### Usage 
* `port-forward` the remote [shared] Prometheus `Service` to your local machine on port `9011`
```
kubectl port-forward -n <namespace> svc/<prometheus-server> 9011:80
```

* `port-forward` the remote [shared] `opencost` `Service` to your local machine on port `9007`
```
kubectl port-forward -n <namespace> svc/<opencost-service> 9007:9003
```

* Run the test with `KUBECOST_NAMESPACE=<namespace>` and turn off timeout: 
```
KUBECOST_NAMESPACE=kubecost go test ./... -v -timeout=0
```
