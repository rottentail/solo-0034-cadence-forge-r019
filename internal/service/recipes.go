package service

import "cadenceforge/internal/domain"

func (s *Service) SaveRecipe(id string, expected int, input domain.Recipe) (domain.Recipe, error) {
	if err := domain.ValidateRecipe(input); err != nil {
		return domain.Recipe{}, err
	}
	s.mutation.Lock()
	defer s.mutation.Unlock()
	if id == "" {
		id = ID()
	}
	var out domain.Recipe
	_, err := s.Persistence.Commit(func(st *domain.State) error {
		versions := st.Recipes[id]
		if len(versions) != expected {
			return domain.Conflict("recipe revision changed")
		}
		if expected > 0 && len(versions) == 0 {
			return domain.Missing("recipe", id)
		}
		input.ID = id
		input.Revision = len(versions) + 1
		out = input
		st.Recipes[id] = append(versions, input)
		st.Emit(id, "recipe.revised", input.Format)
		return nil
	})
	return out, err
}
