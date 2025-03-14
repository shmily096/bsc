select
    'TRANS_ImportExportTransaction' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(STOCreateDT) as from_date,
    max(STOCreateDT) as to_date,
    DATEDIFF(mm,min(STOCreateDT), max(STOCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_ImportExportTransaction
union all

select
    'TRANS_ImportExportDelivery' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(DNCreateDT) as from_date,
    max(DNCreateDT) as to_date,
    DATEDIFF(mm,min(DNCreateDT), max(DNCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_ImportExportDelivery
union all
select
    'TRANS_DeliveryLikp' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(UpdateDT) as from_date,
    max(UpdateDT) as to_date,
    DATEDIFF(mm,min(UpdateDT), max(UpdateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_DeliveryLikp
union all
select
    'TRANS_DomesticDelivery' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(DNCreateDT) as from_date,
    max(DNCreateDT) as to_date,
    DATEDIFF(mm,min(DNCreateDT), max(DNCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_DomesticDelivery
union all
select
    'TRANS_DomesticTransaction' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(STOCeateDT) as from_date,
    max(STOCeateDT) as to_date,
    DATEDIFF(mm,min(STOCeateDT), max(STOCeateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_DomesticTransaction
union all
select
    'TRANS_InventoryOnhand' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min([Date]) as from_date,
    max([Date]) as to_date,
    DATEDIFF(mm,min([Date]), max([Date])) as DiffMonth,
    count(*) as total
from
    TRANS_InventoryOnhand
union all
select
    'TRANS_InventoryTransactions' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(EnterDate) as from_date,
    max(EnterDate) as to_date,
    DATEDIFF(mm,min(EnterDate), max(EnterDate)) as DiffMonth,
    count(*) as total
from
    TRANS_InventoryTransactions
union all
select
    'TRANS_Invoice' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(BillDate) as from_date,
    max(BillDate) as to_date,
    DATEDIFF(mm,min(BillDate), max(BillDate)) as DiffMonth,
    count(*) as total
from
    TRANS_Invoice
union all
select
    'TRANS_PutAway' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(PutAwayDate) as from_date,
    max(PutAwayDate) as to_date,
    DATEDIFF(mm,min(PutAwayDate), max(PutAwayDate)) as DiffMonth,
    count(*) as total
from
    TRANS_PutAway
union all

select
    'TRANS_SalesDelivery' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(DNCreateDT) as from_date,
    max(DNCreateDT) as to_date,
    DATEDIFF(mm,min(DNCreateDT), max(DNCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_SalesDelivery
union all
select
    'TRANS_SalesOrder' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(SOCreateDT) as from_date,
    max(SOCreateDT) as to_date,
    DATEDIFF(mm,min(SOCreateDT), max(SOCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_SalesOrder
union all

select
    'TRANS_ShipmentStatusInbound' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(UpdateDate) as from_date,
    max(UpdateDate) as to_date,
    DATEDIFF(mm,min(UpdateDate), max(UpdateDate)) as DiffMonth,
    count(*) as total
from
    TRANS_ShipmentStatusInbound
union all

select
    'TRANS_T1Invoice' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(MailReceived) as from_date,
    max(MailReceived) as to_date,
    DATEDIFF(mm,min(MailReceived), max(MailReceived)) as DiffMonth,
    count(*) as total
from
    TRANS_T1Invoice
union all

select
    'TRANS_ThirdPartyPurchaseOrder' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(POCreateBy) as from_date,
    max(POCreateBy) as to_date,
    DATEDIFF(mm,min(POCreateBy), max(POCreateBy)) as DiffMonth,
    count(*) as total
from
    TRANS_ThirdPartyPurchaseOrder
union all
select
    'TRANS_Workorder' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(WorkorderCreateDT) as from_date,
    max(WorkorderCreateDT) as to_date,
    DATEDIFF(mm,min(WorkorderCreateDT), max(WorkorderCreateDT)) as DiffMonth,
    count(*) as total
from
    TRANS_Workorder

union all

select
    'TRANS_QuotaDealerPurchase' as table_name,
    MAX(UpdateDT) as laste_update_date,
    min(ContractStartDate) as from_date,
    max(ContractEndDate) as to_date,
    DATEDIFF(mm,min(ContractStartDate), max(ContractEndDate)) as DiffMonth,
    count(*) as total
from
    TRANS_QuotaDealerPurchase

;