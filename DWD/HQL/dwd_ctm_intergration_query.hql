-- Hive SQL
-- Function： CFDA数据 （DWD 层）
-- History: 
-- 2021-11-16    Amanda   v1.0    draft

drop table if exists dwd_ctm_intergrationquery;
create external table dwd_ctm_intergrationquery
(
    CostCenter                            string
    ,DivisionNumber                        string
    ,Forwarder_ReferenceID                 string
    ,Shipment_Number                       string
    ,BLNo                                  string
    ,ShipmentCreateDate                    string
    ,InternalCCSNo                         string
    ,CCSNo                                 string
    ,HBNo                                  string
    ,RegisteredItemID                      string
    ,ImportExport                          string
    ,TransportationMethod                  string
    ,TradeMode                             string
    ,TradingCountryRegion                  string
    ,CCSCreateDate                         string
    ,ImportExportDate                      string
    ,DeclarationDate                       string
    ,CommercialInvoiceNo                   string
    ,PartNumber                            string
    ,HSCode                                string
    ,ChineseDescription                    string
    ,EnglishPart                           string
    ,BOMVersion                            string
    ,DescriptionBondedFlag                 string
    ,OriginCountry                         string
    ,DestinationCountryRegion              string
    ,OriginDestinationCountry              string
    ,DeclQty                               decimal(9,2)
    ,CustomsUM                             string
    ,DeclaredUnitPrice                     string
    ,Currency                              string
    ,DeclarationPrice                      string
    ,TotalAmount                           string
    ,CustomsBOMVersion                     string
    ,Qty1                                  decimal(9,2)
    ,Unit1                                 string
    ,DECLAREPRICE1                         string
    ,Qty2                                  decimal(9,2)
    ,Unit2                                 string
    ,SenderReceiver                        string
    ,NetWeight                             string
    ,PONumber                              string
    ,DPP                                   string
    ,CountryTax                            string
    ,ProvisionalTax                        string
    ,CustomsDutyRate                       string
    ,ActualTaxRate                         string
    ,ProtocolType                          string
    ,VATRate                               string
    ,ConsumptionTaxRate                    string
    ,EstimatedCustomsDuty                  decimal(9,2)
    ,EstimatedVAT                          decimal(9,2)
    ,EstimatedConsumptionTax               string
    ,ShipmentType                          string
    ,Forwarder                             string
    ,Broker                                string
    ,DeclareMode                           string
    ,BillTo                                string
    ,HBLHAWB                               string
    ,CommercialInvoicePrice                string
    ,CommercialInvoiceCurrency             string
    ,BondedManifestDeclareDate             string
    ,BondedManifestDuMark                  string
    ,BMCustomsDeclarationType              string
    ,RelationCCS                           string
    ,DeclarationForm                       string
    ,ListingNumber                         string
    ,AntiDumpingDuty                       decimal(9,2)
    ,ContractNo                            string
    ,UPN                                   string
) comment 'CTMintergrationquery'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_ctm_intergrationquery/'
tblproperties ("parquet.compression"="lzo");