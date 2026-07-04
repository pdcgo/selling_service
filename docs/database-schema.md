# Database Schema & Model

Schema and Model that have :
1. Shop

## Shop Schema
1. Shop must have
    - team_id                   <-- for scoped
    - name
    - username
    - marketplace_type
    - uri
    - created and updated at
    - is deleted                <--- for soft delete
2. (`username`, `team_id`) is unique