package tenant

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func strKV(key, val string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}},
	}
}

func TestResolveDefaultFallback(t *testing.T) {
	r := Resolver{}
	if got := r.Resolve(nil); got != "default" {
		t.Fatalf("expected 'default' for nil resource, got %q", got)
	}
	if got := r.Resolve(&resourcepb.Resource{}); got != "default" {
		t.Fatalf("expected 'default' for empty resource, got %q", got)
	}
}

func TestResolveAttributeBased(t *testing.T) {
	r := Resolver{
		Strategy: "attribute_based",
		Attrs:    []string{"tenant.id", "k8s.namespace.name"},
		Default:  "fallback",
	}

	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strKV("service.name", "payment-svc"),
		strKV("tenant.id", "team-payments"),
	}}
	if got := r.Resolve(res); got != "team-payments" {
		t.Fatalf("expected 'team-payments', got %q", got)
	}

	// Matches second attribute.
	res2 := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strKV("k8s.namespace.name", "prod-checkout"),
	}}
	if got := r.Resolve(res2); got != "prod-checkout" {
		t.Fatalf("expected 'prod-checkout', got %q", got)
	}

	// No match → fallback.
	res3 := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strKV("service.name", "orphan"),
	}}
	if got := r.Resolve(res3); got != "fallback" {
		t.Fatalf("expected 'fallback', got %q", got)
	}
}

func TestResolveStatic(t *testing.T) {
	r := Resolver{Strategy: "static", Default: "solo"}
	// Static ignores resource content entirely.
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("tenant.id", "other")}}
	if got := r.Resolve(res); got != "solo" {
		t.Fatalf("expected 'solo', got %q", got)
	}
}

func TestResolveBatchTraces(t *testing.T) {
	r := Resolver{
		Strategy: "attribute_based",
		Attrs:    []string{"tenant.id"},
		Default:  "unassigned",
	}

	type fakeRS struct {
		res *resourcepb.Resource
		id  int
	}

	items := []fakeRS{
		{id: 1, res: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("tenant.id", "t1")}}},
		{id: 2, res: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("tenant.id", "t2")}}},
		{id: 3, res: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("tenant.id", "t1")}}},
		{id: 4, res: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("other", "x")}}}, // fallback
	}

	groups := ResolveBatch(items, func(it fakeRS) *resourcepb.Resource { return it.res }, r)

	if len(groups) != 3 {
		t.Fatalf("expected 3 tenant groups, got %d", len(groups))
	}
	if len(groups["t1"]) != 2 {
		t.Fatalf("expected 2 items in t1, got %d", len(groups["t1"]))
	}
	if len(groups["t2"]) != 1 {
		t.Fatalf("expected 1 item in t2, got %d", len(groups["t2"]))
	}
	if len(groups["unassigned"]) != 1 {
		t.Fatalf("expected 1 unassigned, got %d", len(groups["unassigned"]))
	}
}

func TestResolveNonStringAttribute(t *testing.T) {
	r := Resolver{Strategy: "attribute_based", Attrs: []string{"tenant.id"}, Default: "fallback"}
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		{Key: "tenant.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
	}}
	if got := r.Resolve(res); got != "fallback" {
		t.Fatalf("expected fallback for non-string attr, got %q", got)
	}
}

func TestResolveWellKnownKeys(t *testing.T) {
	// Resolver with zero Attrs uses well-known keys.
	r := Resolver{Strategy: "attribute_based"}
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strKV("k8s.namespace.name", "production"),
	}}
	if got := r.Resolve(res); got != "production" {
		t.Fatalf("expected well-known key resolution, got %q", got)
	}
}
