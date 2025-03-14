# Buffer

非分拨： 进

## D835

1. 非buffer
   * Actual = 销QTY *TP * Duty Rate
   * Inventory Accrual Duty = 季度末InventQty * TP * Duty Rate
2. Buffer
   1. Actual = 销QTY *TP * Duty Rate
   2. Inventory Accrual Duty = (季度末InventQty  - 季度末Buffer Qty) * TP * Duty Rate

## D838

1. 非buffer

   * Actual = 进QTY *TP * Duty Rate
   * Inventory Accrual Duty = 季度末InventQty * TP * Duty Rate

2. Buffer

   1. Actual = 进QTY *TP * Duty Rate  

      (Buffer from ~ Buffer to ) 按 销QTY *TP * Duty Rate

      其他月份：进QTY *TP * Duty Rate  

   2. Inventory Accrual Duty = (季度末InventQty  - 季度末Buffer Qty) * TP * D



## Buffer 数据

标记未Y

计算时间在 当前时间

c% = 15%