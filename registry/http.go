package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Handler wraps a Service in a Confluent-shaped HTTP API.
//
// Routes:
//
//	POST   /subjects/{subject}/versions       — register a schema
//	GET    /subjects                          — list subjects
//	GET    /subjects/{subject}/versions       — list versions
//	GET    /subjects/{subject}/versions/{v}   — fetch (v: number or "latest")
//	DELETE /subjects/{subject}                — soft-delete via tombstone
//	GET    /schemas/ids/{id}                  — fetch by global ID
//	POST   /compatibility/subjects/{subject}/versions/latest — check compat
//
// Errors are returned as JSON with `error_code` and `message` fields, also
// matching Confluent's shape so existing clients work without changes.
//
// Authentication is opt-in via WithAPIKeys: when configured, every
// request must carry an `Authorization: Bearer <key>` header whose
// value matches one of the registered keys; otherwise 401.
type Handler struct {
	svc     *Service
	apiKeys map[string]struct{}
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithAPIKeys requires every request to carry an `Authorization: Bearer
// <key>` header whose token matches one of the configured keys. Empty
// list disables auth (the default). Mirrors broker.embed.WithAPIKeys
// for consistency across holocron's auth surface.
func WithAPIKeys(keys ...string) HandlerOption {
	return func(h *Handler) {
		h.apiKeys = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			if k != "" {
				h.apiKeys[k] = struct{}{}
			}
		}
	}
}

// NewHandler returns an http.Handler wrapping svc.
func NewHandler(svc *Service, opts ...HandlerOption) *Handler {
	h := &Handler{svc: svc}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// ServeHTTP routes requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="holocron-registry"`)
		writeError(w, http.StatusUnauthorized, 40101, "missing or invalid API key")
		return
	}

	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	switch {
	case len(parts) == 1 && parts[0] == "subjects" && r.Method == http.MethodGet:
		h.listSubjects(w, r)
	case len(parts) == 2 && parts[0] == "subjects" && r.Method == http.MethodDelete:
		h.deleteSubject(w, r, parts[1])
	case len(parts) == 3 && parts[0] == "subjects" && parts[2] == "versions" && r.Method == http.MethodGet:
		h.listVersions(w, r, parts[1])
	case len(parts) == 3 && parts[0] == "subjects" && parts[2] == "versions" && r.Method == http.MethodPost:
		h.register(w, r, parts[1])
	case len(parts) == 4 && parts[0] == "subjects" && parts[2] == "versions" && r.Method == http.MethodGet:
		h.getVersion(w, r, parts[1], parts[3])
	case len(parts) == 3 && parts[0] == "schemas" && parts[1] == "ids" && r.Method == http.MethodGet:
		h.getByID(w, r, parts[2])
	case len(parts) == 5 && parts[0] == "compatibility" && parts[1] == "subjects" &&
		parts[3] == "versions" && parts[4] == "latest" && r.Method == http.MethodPost:
		h.checkCompatibility(w, r, parts[2])
	default:
		writeError(w, http.StatusNotFound, 40401, fmt.Sprintf("unknown path %s %s", r.Method, r.URL.Path))
	}
}

// authorized reports whether r carries a valid API key. Always true when
// no keys are configured (auth disabled).
func (h *Handler) authorized(r *http.Request) bool {
	if len(h.apiKeys) == 0 {
		return true
	}
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	_, ok := h.apiKeys[strings.TrimPrefix(auth, prefix)]
	return ok
}

// deleteSubject implements DELETE /subjects/{subject}.
func (h *Handler) deleteSubject(w http.ResponseWriter, r *http.Request, subject string) {
	if err := h.svc.DeleteSubject(r.Context(), subject); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": subject})
}

func (h *Handler) listSubjects(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.ListSubjects())
}

func (h *Handler) listVersions(w http.ResponseWriter, _ *http.Request, subject string) {
	versions, err := h.svc.ListVersions(subject)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request, subject string) {
	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, 42201, "invalid request body")
		return
	}
	if body.Schema == "" {
		writeError(w, http.StatusBadRequest, 42201, "schema field is required")
		return
	}
	id, err := h.svc.Register(r.Context(), subject, body.Schema)
	if err != nil {
		writeError(w, http.StatusInternalServerError, 50001, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"id": id})
}

func (h *Handler) getVersion(w http.ResponseWriter, _ *http.Request, subject, versionStr string) {
	var sc Schema
	var err error
	if versionStr == "latest" {
		sc, err = h.svc.GetLatest(subject)
	} else {
		v, perr := strconv.Atoi(versionStr)
		if perr != nil {
			writeError(w, http.StatusBadRequest, 42202, fmt.Sprintf("invalid version %q", versionStr))
			return
		}
		sc, err = h.svc.GetVersion(subject, v)
	}
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (h *Handler) getByID(w http.ResponseWriter, _ *http.Request, idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, 42203, fmt.Sprintf("invalid id %q", idStr))
		return
	}
	sc, err := h.svc.GetByID(id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"schema": sc.Schema})
}

func (h *Handler) checkCompatibility(w http.ResponseWriter, r *http.Request, subject string) {
	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, 42201, "invalid request body")
		return
	}
	mode := Compatibility(r.URL.Query().Get("mode"))
	ok, err := h.svc.CheckCompatibility(subject, body.Schema, mode)
	if err != nil {
		writeError(w, http.StatusNotImplemented, 50002, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"is_compatible": ok})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status, code int, message string) {
	writeJSON(w, status, map[string]any{
		"error_code": code,
		"message":    message,
	})
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSubjectNotFound):
		writeError(w, http.StatusNotFound, 40401, err.Error())
	case errors.Is(err, ErrVersionNotFound):
		writeError(w, http.StatusNotFound, 40402, err.Error())
	case errors.Is(err, ErrSchemaNotFound):
		writeError(w, http.StatusNotFound, 40403, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, 50001, err.Error())
	}
}
