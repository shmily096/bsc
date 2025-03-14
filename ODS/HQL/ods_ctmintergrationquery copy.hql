-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-15    Amanda   v1.0    draft

drop table if exists ods_ctm_intergrationquery;
create external table ods_ctm_intergrationquery
(
    UpdateDT                              string
    ,Active                                string
    ,CostCenter                            string
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
    ,Incoterms                             string
    ,CCSCreateDate                         string
    ,ImportExportDate                      string
    ,DeclarationDate                       string
    ,EntryOperator                         string
    ,TrackingOperator                      string
    ,CommercialInvoiceNo                   string
    ,CommercialInvoiceSequenceNumber       string
    ,CustomsManifestDetailNo               string
    ,CCSItemNumber                         string
    ,CustomsItemNo                         string
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
    ,ProjectNumber                         string
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
    ,MOFCOMApprovalNumber                  string
    ,ItemGroupId                           string
    ,VoyageNumber                          string
    ,UsageType                             string
    ,UsageCode                             string
    ,ShipmentType                          string
    ,Forwarder                             string
    ,Broker                                string
    ,DeclareMode                           string
    ,BillTo                                string
    ,HBLHAWB                               string
    ,MBLMAWB                               string
    ,Planner                               string
    ,RegistrationNumber                    string
    ,TrackingID                            string
    ,SITE                                  string
    ,TradingCompanyCode                    string
    ,TradingCompanyName                    string
    ,ReceiverCompanyCode                   string
    ,ReceiverCompanyName                   string
    ,FinanceInvoiceNumber                  string
    ,POSequence                            string
    ,SIDRemark                             string
    ,BrokerPOANumber                       string
    ,CommercialInvoicePrice                string
    ,CommercialInvoiceCurrency             string
    ,BondedManifest                        string
    ,BondedManifestDeclareDate             string
    ,BondedManifestDuMark                  string
    ,BMCustomsDeclarationSign              string
    ,BMCustomsDeclarationType              string
    ,RelationCCS                           string
    ,DeclarationForm                       string
    ,ListingNumber                         string
    ,AntiDumpingDuty                       decimal(9,2)
    ,ContractNo                            string
    ,UPN                                   string
) comment 'CTMintergrationquery'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_ctm_intergrationquery/'
;