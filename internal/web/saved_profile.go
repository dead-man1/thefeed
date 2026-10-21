package web

// activeProfile returns the currently-active profile, or nil if none.
func (s *Server) activeProfile() *Profile {
	pl, err := s.loadProfiles()
	if err != nil || pl == nil {
		return nil
	}
	for i := range pl.Profiles {
		if pl.Profiles[i].ID == pl.Active {
			return &pl.Profiles[i]
		}
	}
	return nil
}

// domainInfo reports the current nickname of an imported config with this
// domain and whether any such config exists.
func (s *Server) domainInfo(domain string) (nickname string, available bool) {
	pl, err := s.loadProfiles()
	if err != nil || pl == nil {
		return "", false
	}
	for _, p := range pl.Profiles {
		if p.Config.Domain == domain {
			return p.Nickname, true
		}
	}
	return "", false
}

// savedItemOut is a SavedItem plus the per-request, config-relative fields the
// UI needs (never stored).
type savedItemOut struct {
	SavedItem
	ConfigLabel string `json:"configLabel"`
	Available   bool   `json:"available"`
	IsActive    bool   `json:"isActive"`
}

// enrichSaved attaches configLabel/available/isActive given the active domain.
func (s *Server) enrichSaved(it SavedItem, activeDomain string) savedItemOut {
	// Authored items (note/file/chat) aren't tied to a config: always available,
	// never config-active, labelled by their own snapshot (e.g. contact name).
	if it.Kind != "" && it.Kind != "bookmark" {
		return savedItemOut{SavedItem: it, ConfigLabel: it.Nickname, Available: true, IsActive: false}
	}
	nickname, available := s.domainInfo(it.Domain)
	label := nickname // current nickname of the still-imported config
	if label == "" {
		label = it.Nickname // snapshot from save time
	}
	if label == "" {
		label = it.Domain // last resort
	}
	return savedItemOut{
		SavedItem:   it,
		ConfigLabel: label,
		Available:   available,
		IsActive:    activeDomain != "" && it.Domain == activeDomain,
	}
}
