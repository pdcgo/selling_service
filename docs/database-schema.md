# Database Schema & Model For Selling Service.



Schema and Model that have :
1. Shop


## Shop Schema
> Model: `selling_models/shop.go` (`Shop` + `ShopUser`, tables `v2_shops` / `v2_shop_users`).

1. Shop must have
    - team_id                   <-- for scoped
    - name
    - username
    - marketplace_type
    - uri
    - created and updated at
    - is deleted                <--- for soft delete
2. (`username`, `team_id`) is unique


## Database Model guideline
1. for model database dont import from legacy model in `shared`
2. write all definition in `./selling_models`

<br><br><br><br><br><br>

# Database Schema & Model For Revenue Service.