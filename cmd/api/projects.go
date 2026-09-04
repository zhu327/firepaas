// projects.go：v1.5 项目 CRUD（最小可用）。
//
//	POST   /v1/projects        创建（全局 admin；非幂等，已存在 409）
//	GET    /v1/projects        列表（全局=全部；受限=仅本项目）
//	GET    /v1/projects/{id}   详情（全局任意；受限仅本项目，gate 在中间件）
//	DELETE /v1/projects/{id}   删除（全局 admin；非空 409，不存在 404）
//
// 配额/限流仍走既有 governance 端点；成员模型暂不引入（apikey project 绑定
// + RBAC 角色即协作边界），见方案说明。
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

var projectIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

type createProjectBody struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (a *API) createProject(w http.ResponseWriter, r *http.Request) {
	if !identityIsGlobal(identFrom(r)) {
		writeErr(w, 403, "project creation requires a global identity (root token or an unscoped admin key)")
		return
	}
	var b createProjectBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if !projectIDPattern.MatchString(b.ID) {
		writeErr(w, 400, "id must match ^[a-z0-9][a-z0-9-]{0,63}$")
		return
	}
	if b.Name == "" {
		b.Name = b.ID
	}
	p, err := a.store.CreateProject(r.Context(), b.ID, b.Name)
	if err != nil {
		if errors.Is(err, store.ErrProjectExists) {
			writeErr(w, 409, "project already exists: "+b.ID)
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 201, p)
}

func (a *API) listProjects(w http.ResponseWriter, r *http.Request) {
	all, err := a.store.ListProjects(r.Context())
	if err != nil {
		writeInternalErr(w, r, err)
		return
	}
	id := identFrom(r)
	if !identityIsGlobal(id) {
		filtered := all[:0:0]
		for _, p := range all {
			if p.ID == id.ProjectID {
				filtered = append(filtered, p)
			}
		}
		all = filtered
		if all == nil {
			all = []store.Project{}
		}
	}
	if all == nil {
		all = []store.Project{}
	}
	writeJSON(w, 200, map[string]any{"projects": all})
}

func (a *API) getProject(w http.ResponseWriter, r *http.Request) {
	p, err := a.store.GetProject(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, 404, "project not found")
			return
		}
		writeInternalErr(w, r, err)
		return
	}
	writeJSON(w, 200, p)
}

func (a *API) deleteProject(w http.ResponseWriter, r *http.Request) {
	if !identityIsGlobal(identFrom(r)) {
		writeErr(w, 403, "project deletion requires a global identity (root token or an unscoped admin key)")
		return
	}
	id := r.PathValue("id")
	if id == "dev" {
		writeErr(w, 409, "refusing to delete the default project dev")
		return
	}
	if err := a.store.DeleteProjectIfEmpty(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeErr(w, 404, "project not found")
		case errors.Is(err, store.ErrProjectNotEmpty):
			writeErr(w, 409, "project not empty: remove apps/volumes/snapshots/pins/keys first")
		default:
			writeInternalErr(w, r, err)
		}
		return
	}
	writeJSON(w, 200, map[string]string{"id": id, "status": "deleted"})
}
