package main

import (
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// registerDashboard is filled in by the demo layer (web dashboard).
func registerDashboard(mux *http.ServeMux, c client.Client) {}
