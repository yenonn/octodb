// Package tenant resolves tenant identity from OTel Resource attributes at ingestion time.
//
// Implementation of ADR-003: Dynamic Tenant Resolution from Resource Attributes.
package tenant

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Resolver extracts tenant ID from an OTel Resource. Zero value is usable and
// falls back to the "default" strategy.
type Resolver struct {
	Strategy string   // "attribute_based", "static", or "default" (default)
	Attrs    []string // ordered list of attribute keys to check; first match wins
	Default  string   // fallback when no attribute matches (default "default")
}

// Resolve returns the tenant ID for a given OTel Resource.
//
// Strategy "attribute_based" (default) walks the configured Attrs in order and
// returns the first string-valued match.  If the resource is nil or no attribute
// matches, Default is returned.
//
// Strategy "static" always returns Default regardless of resource content —
// useful for single-tenant deployments.
func (r Resolver) Resolve(resource *resourcepb.Resource) string {
	strategy := r.Strategy
	if strategy == "" {
		strategy = "default"
	}
	switch strategy {
	case "attribute_based":
		return r.resolveFromAttributes(resource)
	case "static":
		return r.defaultTenant()
	default: // "default"
		return r.defaultTenant()
	}
}

func (r Resolver) defaultTenant() string {
	if r.Default != "" {
		return r.Default
	}
	return "default"
}

func (r Resolver) resolveFromAttributes(resource *resourcepb.Resource) string {
	if resource == nil || len(resource.Attributes) == 0 {
		return r.defaultTenant()
	}
	// Fast path: if no custom Attrs configured, rely on well-known OTel keys.
	attrs := r.Attrs
	if len(attrs) == 0 {
		attrs = []string{"tenant.id", "tenant_id", "k8s.namespace.name", "service.namespace"}
	}
	for _, wantKey := range attrs {
		for _, kv := range resource.Attributes {
			if kv.Key != wantKey {
				continue
			}
			if s := stringValue(kv); s != "" {
				return s
			}
		}
	}
	return r.defaultTenant()
}

// ResolveBatch groups a slice of items that each carry a *resourcepb.Resource
// into per-tenant buckets. The extractFunc pulls the resource from each item.
//
// Typical usage from the gRPC server:
//
//	groups := tenant.ResolveBatch(req.ResourceSpans, func(rs *tracepb.ResourceSpans) *resourcepb.Resource {
//	    return rs.Resource
//	})
//	for tenantID, spans := range groups {
//	    store.WriteTraces(ctx, tenantID, spans)
//	}
func ResolveBatch[T any](items []T, extractFunc func(T) *resourcepb.Resource, resolver Resolver) map[string][]T {
	groups := make(map[string][]T, 1) // common case is single-tenant
	for _, it := range items {
		res := extractFunc(it)
		tid := resolver.Resolve(res)
		groups[tid] = append(groups[tid], it)
	}
	return groups
}

func stringValue(kv *commonpb.KeyValue) string {
	if kv == nil || kv.Value == nil {
		return ""
	}
	switch v := kv.Value.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	default:
		return ""
	}
}
