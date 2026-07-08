# Selling Service
This Is part of Submodule of [Warehouse Infra](https://github.com/pdcgo/warehouse_infra). In Warehouse Infra this is live in folder `./selling_service`.<br>
This Service planned and Intended for replacing legacy Product System that exists in Warehouse Infra. Its planned for microservice and planned to more dependentless, separating domain purpose for better developing big and complex system that exists in Warehouse Infra.<br>
For now its just be candidate for Take over Selling System In Warehouse Infra legacy.
Status for this development is still in progress and not completely take over legacy system.

1. for database schema related, read this [Database Schema](database-schema.md).


## Authentication & Authorization.
1. Use v2 roling system. not legacy system. for complete reference read [this](../../user_service/docs/readme.md#authentication--authorization)
2. use interceptor that live in [here](../../user_service/access_interceptors/interceptor.go)
3. DON'T use legacy interface on [this](../../shared/interfaces/authorization_iface/authorization.go)

## Connect RPC Spec
`SellingService` heavyly depend `connect-rpc` to serve and creating apis and grpc. Why we use `connectrpc` because its can be two mode as pure grpc and grpc-web that interact like web. And also supported http2. This service have several rpc:

1. Shop Management Related RPC
    - Create Shop that named `ShopCreate`
    - Delete Shop that named `ShopDelete`
    - List Shop that named `ShopList`
    - Update Shop that named `ShopUpdate`

    for User management That Who can Access The Shop.
    - list user that have access on shop that named `ShopUserList`.
    - for remove and add user in list of shop. rpc that named `ShopUserUpdate`.



## Shop Management RPC
1. this rpc is team scoped.
2. how dealing team_id and username soft delete: 
    - to prevent error when same shop reinserted, before delete update username to `[username]_deleted_[unix_second_timestamp]`
3. ensure this rpc using v2 roling system & they have access role definition on proto

<br><br><br><br><br><br>

# Revenue Service.

## General Brief.
For this moment this service live in `./selling_service/revenue` and sharing models with selling service. 
1. the service named `RevenueService`.
2. the proto definition is under `selling_iface`


## Authentication & Authorization.
1. Use v2 roling system. not legacy system. for complete reference read [this](../../user_service/docs/readme.md#authentication--authorization)
2. use interceptor that live in [here](../../user_service/access_interceptors/interceptor.go)
3. DON'T use legacy interface on [this](../../shared/interfaces/authorization_iface/authorization.go)

## Connect RPC Spec
`RevenueService` heavyly depend `connect-rpc` to serve and creating apis and grpc. Why we use `connectrpc` because its can be two mode as pure grpc and grpc-web that interact like web. And also supported http2. This service have several rpc:

1. Rpc for Withdrawal Related.
    - for creating withdrawal that named `WithdrawalCreate`.
    - for updating withdrawal that named `WithdrawalUpdate`.
    - for updating withdrawal that named `WithdrawalDelete`.
    - for view withdrawal change log that named `WithdrawalLogList`.

2. Rpc for Revenue Related. 
    - for creating revenue that named `RevenueCreate`.
    - for updating revenue that named `RevenueUpdate`.
    - for updating revenue that named `RevenueDelete`.
    - for view revenue change log that named `RevenueLogList`.

## Withdrawal RPC.

### General.
1. Withdrawal just can be edited if created_at > 7 days ago.

### Structure.
1. Schema. i just listed most important field related withdrawal.
    - withdrawal :
    ```
    type Withdrawal struct {
        Id                  uint64                   <-- primary key with auto increment
        ShopId              uint64
        TeamId              uint64
        Amount              float64 
        FundedAt            time.Time
        RefId               string               <-- that unique together with source, when its delete, change RefId with [ref_id]_deleted_[unix timestamp second]
        Source              WithdrawalSource     <-- from proto definition generated, source is manual or importer
        Note                string
        DeletedAt           time.Time            <-- for soft delete
        UpdatedAt           time.Time
        CreatedAt           time.Time
    }

    type WithdrawalLog struct {
        Id              uint64                               <-- primary key with auto increment
        WithdrawalId    uint64                               <-- foreign key with Withdrawal
        ShopId          uint64
        TeamId          uint64
        ChangeType      WithdrawalLogChangeType              <-- from proto definition generated. it has type: change_amount, deleted, and created
        Note            string
        Amount          float64 
        CreatedAt       time.Time
    }

    ```

### Flow.
1. Flow `WithdrawalCreate`. 
    ```mermaid
    stateDiagram-v2
    [*] --> Open_Transaction
    Open_Transaction --> Payload
    Finish_transaction

    Payload --> Have_Manual_Source
    Payload --> Have_Importer_Source
    Have_Manual_Source --> Withdrawal: RefId must not empty
    Have_Importer_Source --> Withdrawal: calculate RefId from #shopid/year/month/day
    Withdrawal --> WithdrawalLog: create
    Withdrawal --> Finish_transaction: save
    WithdrawalLog --> Finish_transaction: save
    Finish_transaction --> [*]

    ```

2. Flow `WithdrawalUpdate`.
    ```mermaid
    stateDiagram-v2

    [*] --> Open_Transaction
    Open_Transaction --> Payload
    Payload --> WithdrawalLog
    Payload --> Update_Withdrawal
    WithdrawalLog --> Finish_transaction: save
    Update_Withdrawal --> Finish_transaction: atomic
    
    Finish_transaction --> [*]


    ```

3. Flow `WithdrawalDelete`.
    ```mermaid
    stateDiagram-v2

    [*] --> Open_Transaction
    Open_Transaction --> Payload
    Payload --> WithdrawalLog
    Payload --> Delete_Withdrawal
    WithdrawalLog --> Finish_transaction: save
    Delete_Withdrawal --> Finish_transaction
    
    Finish_transaction --> [*]


    ```


## Revenue RPC. [defered to build, plan still not final]

### Structure.
1. Schema. i just listed most important field related Revenue.
    - OrderRevenue:
        ```
        type OrderRevenue struct {
            Id  uint64                   <-- primary key with auto increment
            OrderId             uint64
            ShopId              uint64
            TeamId              uint64
            Amount              float64 
            FundedAt            time.Time
            RefId               string               <-- that unique together with source, when its delete, change RefId with [ref_id]_deleted_[unix timestamp second]
            Source              WithdrawalSource     <-- from proto definition generated, source is manual or importer
            DeletedAt           time.Time            <-- for soft delete
            UpdatedAt           time.Time
            CreatedAt           time.Time

        }
        
        ```
    - OrderRevenueLog
        ```
        type OrderRevenueLog struct {
            Id  uint64                   <-- primary key with auto increment
            CreatedAt           time.Time

        }

        ```
    - AdjustmentRevenue


### Flow.
1. Flow `RevenueCreate`


    