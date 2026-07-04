# Selling Service
This Is part of Submodule of [Warehouse Infra](https://github.com/pdcgo/warehouse_infra). In Warehouse Infra this is live in folder `./selling_service`.<br>
This Service planned and Intended for replacing legacy Product System that exists in Warehouse Infra. Its planned for microservice and planned to more dependentless, separating domain purpose for better developing big and complex system that exists in Warehouse Infra.<br>
For now its just be candidate for Take over Selling System In Warehouse Infra legacy.
Status for this development is still in progress and not completely take over legacy system.

1. for database schema related, read this [Database Schema](database-schema.md).


## Authentication & Authorization
1. Use v2 roling system. not legacy system. [Read This](../../user_service/docs/readme.md)

## Connect RPC Spec
`SellingService` heavyly depend `connect-rpc` to serve and creating apis and grpc. Why we use `connectrpc` because its can be two mode as pure grpc and grpc-web that interact like web. And also supported http2. This service have several rpc:

1. Shop Management Related RPC
    - Create Shop that named `ShopCreate`
    - Delete Shop that named `ShopDelete`
    - List Shop that named `ShopList`
    - Update Shop that named `ShopUpdate`

    for User management That Who can Access The Shop.
    - list user that have access on shop that named `ShopUserList`
    - for remove and add user in list of shop. rpc that named `ShopUserUpdate`



## Shop Management RPC
1. this rpc is team scoped.
2. how dealing team_id and username soft delete: 
    - to prevent error when same shop reinserted, before delete update username to `[username]_deleted_[unix_second_timestamp]`
3. ensure this rpc using v2 roling system & they have access role definition on proto