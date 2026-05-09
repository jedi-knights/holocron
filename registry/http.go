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
//	DELETE /subjects/{subject}/versions/{v}   — soft-delete one version
//	GET    /schemas/ids/{id}                  — fetch by global ID
//	DELETE /schemas/ids/{id}                  — soft-delete by global ID
//	GET    /config/{subject}                  — read per-subject compatibility mode
//	PUT    /config/{subject}                  — set per-subject compatibility mode
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
	case len(parts) == 4 && parts[0] == "subjects" && parts[2] == "versions" && r.Method == http.MethodDelete:
		h.deleteVersion(w, r, parts[1], parts[3])
	case len(parts) == 5 && parts[0] == "subjects" && parts[2] == "versions" && parts[4] == "schema" && r.Method == http.MethodGet:
		h.getVersionSchemaText(w, r, parts[1], parts[3])
	case len(parts) == 3 && parts[0] == "schemas" && parts[1] == "ids" && r.Method == http.MethodGet:
		h.getByID(w, r, parts[2])
	case len(parts) == 4 && parts[0] == "schemas" && parts[1] == "ids" && parts[3] == "schema" && r.Method == http.MethodGet:
		h.getByIDSchemaText(w, r, parts[2])
	case len(parts) == 3 && parts[0] == "schemas" && parts[1] == "ids" && r.Method == http.MethodDelete:
		h.deleteByID(w, r, parts[2])
	case len(parts) == 5 && parts[0] == "compatibility" && parts[1] == "subjects" &&
		parts[3] == "versions" && parts[4] == "latest" && r.Method == http.MethodPost:
		h.checkCompatibility(w, r, parts[2])
	case len(parts) == 2 && parts[0] == "config" && r.Method == http.MethodGet:
		h.getConfig(w, r, parts[1])
	case len(parts) == 2 && parts[0] == "config" && r.Method == http.MethodPut:
		h.putConfig(w, r, parts[1])
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

// getVersionSchemaText implements GET
// /subjects/{subject}/versions/{v}/schema. Returns just the
// schema text (Confluent Schema Registry's bare-text variant of
// the wrapped /versions/{v} endpoint). Useful for clients that
// want to feed the schema directly to a parser without unwrapping
// the JSON envelope.
func (h *Handler) getVersionSchemaText(w http.ResponseWriter, _ *http.Request, subject, versionStr string) {
	sc, err := resolveVersion(h.svc, subject, versionStr)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(sc.Schema))
}

// getByIDSchemaText implements GET /schemas/ids/{id}/schema —
// the bare-text counterpart of /schemas/ids/{id}. Same Confluent
// compat motivation as getVersionSchemaText.
func (h *Handler) getByIDSchemaText(w http.ResponseWriter, _ *http.Request, idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil || id < 0 {
		writeError(w, http.StatusBadRequest, 40001, "invalid id: must be a non-negative integer")
		return
	}
	sc, err := h.svc.GetByID(id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(sc.Schema))
}

// getConfig implements GET /config/{subject}. Returns the
// configured compatibility mode in the Confluent shape:
// {"compatibilityLevel": "BACKWARD"}. Subjects without a
// configured mode return "NONE".
func (h *Handler) getConfig(w http.ResponseWriter, _ *http.Request, subject string) {
	mode := h.svc.GetCompatibility(subject)
	if mode == "" {
		mode = CompatibilityNone
	}
	writeJSON(w, http.StatusOK, map[string]string{"compatibilityLevel": string(mode)})
}

// putConfig implements PUT /config/{subject} with body
// {"compatibility": "BACKWARD"} (or "compatibilityLevel" — both
// accepted for Confluent compatibility). Persists the mode so a
// subsequent Register on the subject runs the matching
// compatibility check automatically.
func (h *Handler) putConfig(w http.ResponseWriter, r *http.Request, subject string) {
	var body struct {
		Compatibility      Compatibility `json:"compatibility"`
		CompatibilityLevel Compatibility `json:"compatibilityLevel"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, 42208, "invalid request body")
		return
	}
	mode := body.Compatibility
	if mode == "" {
		mode = body.CompatibilityLevel
	}
	if err := h.svc.SetCompatibility(r.Context(), subject, mode); err != nil {
		writeError(w, http.StatusBadRequest, 42203, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"compatibility": string(mode)})
}

// deleteByID implements DELETE /schemas/ids/{id}. Resolves the ID
// to a (subject, version) and removes that version via a
// per-version tombstone; sibling versions of the subject and
// every other subject remain intact.
func (h *Handler) deleteByID(w http.ResponseWriter, r *http.Request, idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil || id < 0 {
		writeError(w, http.StatusBadRequest, 40001, "invalid id: must be a non-negative integer")
		return
	}
	if err := h.svc.DeleteByID(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted_id": id})
}

// deleteVersion implements DELETE /subjects/{subject}/versions/{v}.
// Removes only the named version of subject; other versions remain.
func (h *Handler) deleteVersion(w http.ResponseWriter, r *http.Request, subject, version string) {
	v, err := strconv.Atoi(version)
	if err != nil || v <= 0 {
		writeError(w, http.StatusBadRequest, 40001, "invalid version: must be a positive integer")
		return
	}
	if err := h.svc.DeleteVersion(r.Context(), subject, v); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": subject, "version": v})
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
	sc, err := resolveVersion(h.svc, subject, versionStr)
	if err != nil {
		if errors.Is(err, errBadVersionString) {
			writeError(w, http.StatusBadRequest, 42202, fmt.Sprintf("invalid version %q", versionStr))
			return
		}
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

// resolveVersion handles the "latest" alias and the numeric form
// in one place. Sentinel errBadVersionString lets callers map
// parse failures to a 400 instead of the 404 GetVersion would
// otherwise produce.
var errBadVersionString = errors.New("invalid version string")

func resolveVersion(svc *Service, subject, versionStr string) (Schema, error) {
	if versionStr == "latest" {
		return svc.GetLatest(subject)
	}
	v, err := strconv.Atoi(versionStr)
	if err != nil {
		return Schema{}, errBadVersionString
	}
	return svc.GetVersion(subject, v)
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
