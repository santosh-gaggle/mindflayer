<?php

declare(strict_types=1);
// @codeCoverageIgnoreStart
namespace Unilever\IsrDataImporter\Model;

use Exception;
use Magento\Company\Api\Data\CompanyInterface as CompanyStatusInterface;
use Magento\Framework\App\Area;
use Magento\Framework\App\State;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\Exception\LocalizedException;
use Magento\Store\Model\App\Emulation;
use Magento\Store\Model\StoreManagerInterface;
use Magento\Customer\Api\CustomerRepositoryInterface;
use Magento\Customer\Model\CustomerFactory;
use Magento\Customer\Api\AddressRepositoryInterface;
use Magento\Customer\Api\Data\CustomerInterfaceFactory;
use Magento\CustomerGraphQl\Model\Customer\Address\CreateCustomerAddress;
use Magento\CustomerGraphQl\Model\Customer\Address\ExtractCustomerAddressData;
use Magento\Directory\Model\RegionFactory;
use Magento\Company\Api\CompanyManagementInterface;
use Magento\Company\Model\Customer\Company;
use Magento\Company\Model\CompanyFactory;
use Magento\CompanyCredit\Api\CreditLimitRepositoryInterface;
use Magento\CompanyCredit\Api\CreditLimitManagementInterface;
use Magento\CompanyPayment\Model\CompanyPaymentMethodFactory;
use Magento\CompanyPayment\Model\ResourceModel\CompanyPaymentMethod;
use Magento\Store\Api\WebsiteRepositoryInterface;
use Marketplace\Seller\Model\ResourceModel\Seller\Collection as VendorCollection;
use Unilever\DistributorZoneMapping\Model\DistributorZoneMappingRepository;
use Unilever\OutletMasterIntegration\Helper\OutletMasterConfig;
use Unilever\Customer\Model\Customer\Attribute\Source\ActivationStatus;
use Magento\Framework\Registry;
use Magento\Framework\Stdlib\DateTime\DateTime;

class ProcessOutletMasterOptimized
{
    public const APPLICABLE_PAYMENT_METHOD = 2;
    public const DATE_FORMAT = 'Y-m-d H:i:s';
    public const TABLE_CUSTOMER_ENTITY = 'customer_entity';
    public const ERP_SYNC_STATUS_VALUE = 1;
    public const ERP_SYNC_B2B_OUTLET_FLAG = 4;
    public const PARENT_ACCOUNT_TRUE = 1;
    public const PARENT_ACCOUNT_FALSE = 0;
    public const WHITESPCE_CUSTOMER_CODE = 'customer_code';
    public const WHITESPCE_INVITE_CODE = 'invite_code';
    public const WHITESPCE_SELLER_CODE = 'seller_code';
    public const WHITESPCE_COMPANY_EMAIL = 'company_email';
    public const WHITESPCE_TELEPHONE = 'telephone';
    public const WHITESPCE_STATUS = 'whitespace_outlet_status';
    public const TABLE_WHITESPCE = 'unilever_whitespace_outlet';


    public const VENDOR_TABLE = 'marketplace_seller';
    public const CUSTOMER_ENTITY_TABLE = 'customer_entity';
    public const COMPANY_PAYMENT_TABLE = 'company_payment';
    public const UNILEVER_CUSTOMER_SELLER_MAPPING_TABLE = 'unilever_customer_seller_mapping';
    /**
     * @var String
     */
    protected $bunchErrorMessage = [];

    /**
     * Process OutletMaster Constructor
     *
     * @param ResourceConnection $resourceConnection
     * @param StoreManagerInterface $storeManager
     * @param State $state
     * @param Emulation $appEmulation
     * @param CustomerRepositoryInterface $customerRepository
     * @param CustomerFactory $customerFactory
     * @param AddressRepositoryInterface $addressRepository
     * @param CreateCustomerAddress $createCustomerAddress
     * @param ExtractCustomerAddressData $extractCustomerAddressData
     * @param RegionFactory $regionFactory
     * @param CompanyManagementInterface $companyManagement
     * @param Company $companyModel
     * @param CompanyFactory $companyFactory
     * @param CreditLimitRepositoryInterface $creditLimitRepository
     * @param CreditLimitManagementInterface $creditLimitManagement
     * @param CompanyPaymentMethodFactory $companyPaymentMethodFactory
     * @param CompanyPaymentMethod $companyPaymentMethodResource
     * @param WebsiteRepositoryInterface $websiteRepository
     * @param VendorCollection $vendorCollection
     * @param DistributorZoneMappingRepository $distributorZoneMappingRepository
     * @param OutletMasterConfig $outletMasterConfig
     * @param CustomerInterfaceFactory $customerInterfaceFactory
     * @param DateTime $dateTime
     */
    public function __construct(
        public ResourceConnection               $resourceConnection,
        public StoreManagerInterface            $storeManager,
        public State                            $state,
        public Emulation                        $appEmulation,
        public CustomerRepositoryInterface      $customerRepository,
        public CustomerFactory                  $customerFactory,
        public AddressRepositoryInterface       $addressRepository,
        public CreateCustomerAddress            $createCustomerAddress,
        public ExtractCustomerAddressData       $extractCustomerAddressData,
        public RegionFactory                    $regionFactory,
        public CompanyManagementInterface       $companyManagement,
        public Company                          $companyModel,
        public CompanyFactory                   $companyFactory,
        public CreditLimitRepositoryInterface   $creditLimitRepository,
        public CreditLimitManagementInterface   $creditLimitManagement,
        public CompanyPaymentMethodFactory      $companyPaymentMethodFactory,
        public CompanyPaymentMethod             $companyPaymentMethodResource,
        public WebsiteRepositoryInterface       $websiteRepository,
        public VendorCollection                 $vendorCollection,
        public DistributorZoneMappingRepository $distributorZoneMappingRepository,
        public OutletMasterConfig               $outletMasterConfig,
        public CustomerInterfaceFactory         $customerInterfaceFactory,
        public Registry                         $registry,
        public DateTime                         $dateTime,
    ) {
        if (!$this->registry->registry('isSecureArea')) {
            $registry->register('isSecureArea', true);
        }
    }

    public function getVendorData($vendorCode = [])
    {
        $vendorData = [];
        $result = [];
        if (!empty($vendorCode)) {
            $vendorCode = "'" . implode("','", $vendorCode) . "'";
            $where = '(seller_code in (' . $vendorCode . '))'; //phpcs:ignore
            $connection = $this->resourceConnection->getConnection();
            $tableName = $connection->getTableName(self::VENDOR_TABLE);
            $select = $connection->select()
                ->from($tableName, ['seller_id', 'seller_group_id', 'seller_erp_code', 'seller_code'])
                ->where($where);
            $result = $connection->fetchAll($select);
            foreach ($result as $data) {
                $vendorData[$data['seller_code']] = $data;
            }
        }
        return $vendorData;
    }

    public function getExistingCustomerData($customer_code = [], $seller_codes = [], $allVendorData = [])
    {
        $vendorIds = [];
        $existingCustomers = [];
        foreach ($seller_codes as $data) {
            if (isset($allVendorData[$data])) {
                $vendorIds[] = $allVendorData[$data]['seller_id'];
            }
        }
        if (!empty($vendorIds) && !empty($customer_code)) {
            $customer_code = "'" . implode("','", $customer_code) . "'";
            $vendorIds = "'" . implode("','", $vendorIds) . "'";
            $where = '( customer_code in(' . $customer_code . ') AND vendor_id in(' . $vendorIds . '))'; //phpcs:ignore
            $connection = $this->resourceConnection->getConnection();
            $tableName = $connection->getTableName(self::CUSTOMER_ENTITY_TABLE);
            $select = $connection->select()
                ->from($tableName, ['entity_id', 'email', 'customer_code', 'vendor_id', 'created_at', 'default_shipping'])
                ->where($where);
            $result = $connection->fetchAll($select);
            foreach ($result as $data) {
                $existingCustomers[$data['customer_code']] = $data;
            }
        }
        return $existingCustomers;
    }
    public function getExistingCustomerSellerMapping($retailer_id = [], $seller_codes = [])
    {
        $existingCustomers = [];
        if (!empty($retailer_id) && !empty($seller_codes)) {
            $retailer_id = "'" . implode("','", $retailer_id) . "'";
            $seller_codes = "'" . implode("','", $seller_codes) . "'";
            $where = '( retailer_id in(' . $retailer_id . ') AND seller_code in(' . $seller_codes . '))'; //phpcs:ignore
            $connection = $this->resourceConnection->getConnection();
            $tableName = $connection->getTableName(self::UNILEVER_CUSTOMER_SELLER_MAPPING_TABLE);
            $select = $connection->select()
                ->from($tableName, ['entity_id', 'retailer_id', 'seller_code'])
                ->where($where);
            $result = $connection->fetchAll($select);
            foreach ($result as $data) {
                $existingCustomers[$data['retailer_id'] . '_' . $data['seller_code']] = $data['entity_id'];
            }
        }
        return $existingCustomers;
    }

    public function getExistingWhiteSpaceData($customer_code = [])
    {
        $existingCustomers = [];
        if (!empty($customer_code)) {
            $customer_code = "'" . implode("','", $customer_code) . "'";
            $where = '(customer_code in (' . $customer_code . '))'; //phpcs:ignore
            $connection = $this->resourceConnection->getConnection();
            $tableName = $connection->getTableName(self::TABLE_WHITESPCE);
            $select = $connection->select()
                ->from($tableName, ['customer_code', 'entity_id'])
                ->where($where);
            $result = $connection->fetchAll($select);
            foreach ($result as $data) {
                $existingCustomers[$data['customer_code']] = $data['entity_id'];
            }
        }
        return $existingCustomers;
    }
    /**
     * Process Records
     *
     * @param array $outletData
     * @param string $storeId
     * @param mixed $isrApproverId
     * @param object $logger
     * @return array
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function processrecords($outletData, $storeId, $isrApproverId, $logger)
    {
        $logger->info('=====Start Processing Total Records  => ' . count($outletData) . '=====');
        $processingResult = [];
        $statusArray = $this->getStatusArray();
        $ulActivationStatusArray = $this->getUlActivationStatusArray();
        $addressStatusArray = $this->getAddressStatusArray();
        $this->appEmulation->startEnvironmentEmulation($storeId, Area::AREA_FRONTEND, true);
        $store = $this->storeManager->getStore($storeId);
        $storeName = $this->storeManager->getStore($storeId)->getName();
        $websiteId = $store->getWebsiteId();
        $inviteCode = $this->outletMasterConfig->getInviteCode();
        $zoneMapping = $this->outletMasterConfig->getDefaultZone();
        $defaultCustomerGroupId = $this->outletMasterConfig->getDefaultCustomerGroupId();
        $this->bunchErrorMessage = [];
        //1. get all vendor data where  seller_code in
        $seller_codes = array_unique(array_column($outletData, 'seller_code'));
        $allVendorData = $this->getVendorData($seller_codes);
        //2. get all existing customer email and id where customer_code and vendor_id
        $customer_codes = array_column($outletData, 'customer_code');
        $allExistingCustomerData = $this->getExistingCustomerData($customer_codes, $seller_codes, $allVendorData);
        // get existing whitespacedata
        $ExistingWhiteSpaceData = $this->getExistingWhiteSpaceData($customer_codes);
        $prepareInsertData = [];
        $prepareUpdateData = [];
        $formatedOutletData = [];
        $whitespaceOutletDetails = [];
        foreach ($outletData as $row) {
            //3.format row data and prepare array to insert update multiple
            if (empty($row['customer_code'])) {
                $logger->info('-----Customer Code is mandatory-----');
                $this->bunchErrorMessage[] = [$row['company_name'] =>
                __('Column customer_code (%1) is empty.', $row['customer_code'])];
                continue;
            }
            $logger->info('=====Process Start for customer code  => ' . $row['customer_code'] . '=====');
            //updating the row telephone
            $row['telephone'] = $this->updateMobileNumber($row['telephone']);
            //format dob
            $row['dob'] = $this->dateTime->date('Y-m-d', $row['dob']);
            //replacing first name and last name special characters
            $specialCharString = $this->outletMasterConfig->getSpecialCharacterRegularExpression();
            if (!empty($specialCharString)) {
                $row['firstName'] = preg_replace($specialCharString, '', $row['firstName']);
                $row['lastName'] = preg_replace($specialCharString, '', $row['lastName']);
            }
            // setting 0 value if index not set
            if (!isset($row['b2b_customer']) || $row['b2b_customer'] == null) {
                $row['b2b_customer'] = 0;
            }
            $formatedOutletData[] = $row;
            $logger->info('-----Start Processing-----' . $row['customer_code'] . '-----');
            // check for the whitespace outlet or not
            if ($row['whitespace_outlet_status'] == 1 || $row['whitespace_outlet_status'] == "1") {
                $logger->info('-----WhiteSpace Outlet----' . $row['customer_code'] . '-----');
                $data = [];
                $data = [
                    self::WHITESPCE_SELLER_CODE => $row['seller_code'],
                    self::WHITESPCE_COMPANY_EMAIL => $row['company_email'],
                    self::WHITESPCE_TELEPHONE => $row['telephone'],
                    self::WHITESPCE_STATUS => $row['whitespace_outlet_status'],
                    self::WHITESPCE_CUSTOMER_CODE => $row['customer_code'],
                    self::WHITESPCE_INVITE_CODE => $inviteCode
                ];
                if (isset($ExistingWhiteSpaceData[$row['customer_code']])) {
                    $data['entity_id'] = $ExistingWhiteSpaceData[$row['customer_code']];
                }
                $whitespaceOutletDetails[] = $data;
            } else {
                $logger->info('-----No WhiteSpace Outlet----' . $row['customer_code'] . '-----');
                $sync_status = 0;
                $invite_code = $inviteCode;
                if ($row['b2b_customer'] == "1" || $row['b2b_customer'] == 1) {
                    $sync_status = self::ERP_SYNC_B2B_OUTLET_FLAG;
                    $invite_code = '';
                }
                if (isset($allVendorData[$row['seller_code']]['seller_id']) && !isset($allExistingCustomerData[$row['customer_code']])) {
                    $prepareInsertData[] = [
                        'website_id' => $websiteId, 'store_id' => $storeId, 'firstname' => $row['firstName'],
                        'lastname' => $row['lastName'], 'email' => $row['email'], 'dob' => $row['dob'], 'group_id' => $defaultCustomerGroupId,
                        'created_in' => $storeName, 'mobile_number' => $row['telephone'], 'customer_code' => $row['customer_code'],
                        'invite_code' => $invite_code, 'sync_status' => $sync_status,
                        'vendor_id' => $allVendorData[$row['seller_code']]['seller_id'], 'vendor_group_id' => $allVendorData[$row['seller_code']]['seller_group_id'],
                        'zone_mapping' => $zoneMapping
                    ];
                }
                if (isset($allVendorData[$row['seller_code']]['seller_id']) && isset($allExistingCustomerData[$row['customer_code']])) {
                    $prepareUpdateData[] = [
                        'entity_id' => $allExistingCustomerData[$row['customer_code']]['entity_id'], 'website_id' => $websiteId,
                        'store_id' => $storeId, 'firstname' => $row['firstName'],
                        'lastname' => $row['lastName'], 'email' => $row['email'], 'dob' => $row['dob'], 'group_id' => $defaultCustomerGroupId,
                        'created_in' => $storeName, 'invite_code' => $invite_code, 'sync_status' => $sync_status,
                        'vendor_id' => $allVendorData[$row['seller_code']]['seller_id'],
                        'vendor_group_id' => $allVendorData[$row['seller_code']]['seller_group_id'], 'zone_mapping' => $zoneMapping
                    ];
                }
            }
        }
        // insertwhiteSpaceData added unique index on customer code
        if (!empty($whitespaceOutletDetails)) {
            $connection = $this->resourceConnection->getConnection();
            $connection->insertOnDuplicate(self::TABLE_WHITESPCE, $whitespaceOutletDetails);
            unset($whitespaceOutletDetails);
        }
        //3. insert new customer data.
        if (!empty($prepareInsertData)) {
            $connection = $this->resourceConnection->getConnection();
            $connection->insertMultiple(self::CUSTOMER_ENTITY_TABLE, $prepareInsertData);
            unset($prepareInsertData);
        }
        //4. update all with rest of the data.
        if (!empty($prepareUpdateData)) {
            $connection = $this->resourceConnection->getConnection();
            $connection->insertOnDuplicate(
                self::CUSTOMER_ENTITY_TABLE,
                $prepareUpdateData
            );
            unset($prepareUpdateData);
        }
        //5. save attributes
        $allExistingCustomerData = $this->getExistingCustomerData($customer_codes, $seller_codes, $allVendorData);
        $attributesNeedToSave = ['isr_status' => 'status', 'ul_activation_status' => 'status'];
        $attributeDetails = [];
        foreach ($attributesNeedToSave as $key => $value) {
            $attribute = $this->customerFactory->create()->getAttribute($key);
            $attributeDetails[$key] = ['id' => $attribute->getId(), 'table' => $attribute->getBackend()->getTable(), 'mapping' => $value];
        }
        $saveAttributes = [];
        $formatedOutletDataWithCustomerId = [];
        foreach ($formatedOutletData as $row) {
            if ($row['whitespace_outlet_status'] == 1 || $row['whitespace_outlet_status'] == "1") {
                continue;
            }
            if (isset($allVendorData[$row['seller_code']]['seller_id']) && isset($allExistingCustomerData[$row['customer_code']])) {
                $row['customer_id'] = $allExistingCustomerData[$row['customer_code']]['entity_id'];
                $row['created_at'] = $allExistingCustomerData[$row['customer_code']]['created_at'];
                $row['default_shipping'] = $allExistingCustomerData[$row['customer_code']]['default_shipping'];
                $formatedOutletDataWithCustomerId[] =  $row;
                foreach ($attributeDetails as $key => $attrData) {
                    $attvalue = '';
                    if ($key == 'isr_status') {
                        $attvalue = $statusArray[$row[$attrData['mapping']]];
                    } elseif ($key == 'ul_activation_status') {
                        $attvalue = $ulActivationStatusArray[$row[$attrData['mapping']]];
                    } else {
                        $attvalue = $row[$attrData['mapping']];
                    }
                    $saveAttributes[$attrData['table']][] = [
                        'entity_id' =>  $allExistingCustomerData[$row['customer_code']]['entity_id'],
                        'attribute_id' => $attrData['id'],
                        'value' => $attvalue,
                    ];
                }
            }
        }
        //find out attribute id and table name
        if (!empty($saveAttributes)) {
            if ($saveAttributes)
                foreach ($saveAttributes as $tableName => $tableData) {
                    $connection->insertOnDuplicate($tableName, $tableData, ['value']);
                }
            unset($saveAttributes);
        }

        $seller_codes = array_unique(array_column($formatedOutletDataWithCustomerId, 'seller_code'));
        $customer_ids = array_unique(array_column($formatedOutletDataWithCustomerId, 'customer_id'));
        //get primary key for existing customer seller mapping
        $existingCustomerSellerMapping = $this->getExistingCustomerSellerMapping($customer_ids, $seller_codes);
        $companyPaymentData = [];
        $customerSellerMappingInsertData = [];
        //5. update address
        //6. update company 
        if (!empty($formatedOutletDataWithCustomerId)) {
            foreach ($formatedOutletDataWithCustomerId as $row) {
                try {
                    //Create or update Customer Address
                    $customerId = intval($row['customer_id']);
                    $createdAt = $row['created_at'];
                    $default_shipping_id =  $row['default_shipping'];
                    // if country_id is empty or missing use default country_id
                    if (empty($row['country_id'])) {
                        $row['country_id'] = $this->outletMasterConfig->getCountryByWebsite();
                    }
                    try {
                        $addressId = '';
                        $addressArray = [
                            'postcode' => $row['postcode'],
                            'city' => $row['city'],
                            'firstname' => $row['firstName'],
                            'lastname' => $row['lastName'],
                            'street' => [$row['street']],
                            'country_id' => $row['country_id'],
                            'ul_geo_coordinates' => $row['geo_coordinates'],
                            'telephone' => $row['telephone'],
                            'district' => $row['district'],
                            'region' => $this->getRegionId($row['region'], $row['country_id']),
                            'ul_address_status' => $addressStatusArray[$row['status']]
                        ];
                        $defaultShipping = $default_shipping_id;
                        if ($defaultShipping) {
                            $addressId = $defaultShipping;
                            $addressDetails = $this->addressRepository->getById($addressId);
                            $addressDetails->setId($addressId);
                            $addressDetails->setCustomerId($customerId);
                            $addressDetails->setPostcode($row['postcode']);
                            $addressDetails->setCity($row['city']);
                            $addressDetails->setFirstname($row['firstName']);
                            $addressDetails->setLastname($row['lastName']);
                            $addressDetails->setStreet([$row['street']]);
                            $addressDetails->setCountryId($row['country_id']);
                            $addressDetails->setRegion($this->getRegionId($row['region'], $row['country_id']));
                            $addressDetails->setCustomAttribute(
                                'ul_address_status',
                                $addressStatusArray[$row['status']]
                            );
                            $addressDetails->setCustomAttribute(
                                'ul_geo_coordinates',
                                $row['geo_coordinates']
                            );
                            $this->addressRepository->save($addressDetails);
                            $logger->info('-----Customer Address Updated Successfully-----');
                        } else {
                            $address = $this->createCustomerAddress->execute($customerId, $addressArray);
                            $addressData = $this->extractCustomerAddressData->execute($address);
                            $addressId = $addressData['id'];
                            $logger->info('-----Customer Address Created Successfully-----');
                        }
                        if ($addressId) {
                            $addressRepo = $this->addressRepository->getById($addressId)->setCustomerId($customerId);
                            $addressRepo->setIsDefaultShipping(true);
                            $addressRepo->setIsDefaultBilling(true);
                            $this->addressRepository->save($addressRepo);
                        }
                    } catch (\Exception $e) {
                        $logger->info("There was some exception while address Creation." .
                            $e->getMessage());
                        $this->bunchErrorMessage[] = [$row['customer_code'] => "Address Save => " .
                            $e->getMessage()];
                        continue;
                    }

                    //Create or Update Company
                    try {
                        $companyId = '';
                        $companyData = $this->getCompanyDataFromRow($row, $customerId, $isrApproverId);

                        $currentCompanyId = $this->outletMasterConfig->getCompanyId($customerId, $row['customer_code']);
                        if (empty($currentCompanyId) || $currentCompanyId == null) {

                            $logger->info('-----In New Company Creation-----');
                            $customerDetail = $this->customerRepository->getById($customerId);
                            if ($row['status'] == CompanyStatusInterface::STATUS_APPROVED) {
                                $companyData['status'] = CompanyStatusInterface::STATUS_PENDING;
                                if ($row['b2b_customer'] == "1" || $row['b2b_customer'] == 1) {
                                    $companyData['customer_activated_at'] = $createdAt;
                                    $companyData['status'] = CompanyStatusInterface::STATUS_APPROVED;
                                }
                            } else {
                                $companyData['status'] = $statusArray[$row['status']];
                            }
                            $companyData['telephone'] = $row['telephone'];
                            $companyData['mobile'] = $row['telephone'];
                            $companyData['company_email'] = $row['company_email'];
                            $companyData['email'] = $row['email'];
                            $logger->info('-----Before Company Creation-----');
                            $this->companyModel->createCompany($customerDetail, $companyData, null);
                            $companyId = $this->companyManagement->getByCustomerId($customerId)->getId();
                            $logger->info('-----Company Created Successfully-----');
                        } else {
                            $logger->info('-----Company Data Existing-----');
                            $companyId = $currentCompanyId;
                            $companyData['entity_id'] = $currentCompanyId;
                            if ($row['status'] != CompanyStatusInterface::STATUS_APPROVED) {
                                $companyData['status'] = $statusArray[$row['status']];
                            }
                            if ($row['b2b_customer'] == "1" || $row['b2b_customer'] == 1) {
                                if ($row['status'] == CompanyStatusInterface::STATUS_APPROVED) {
                                    $companyData['status'] = CompanyStatusInterface::STATUS_APPROVED;
                                }
                                $companyData['customer_activated_at'] = $createdAt;
                            }

                            //$companyData['is_sync_required'] = 1; // comment this for DataSync issue

                            $companyDetails = $this->companyFactory->create();
                            $companyDetails->setData($companyData)->save();
                            $logger->info('-----Company updated Successfully-----');
                        }
                    } catch (LocalizedException $e) {
                        $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Localized exception on company save => " .
                            $e->getMessage()];
                    } catch (Exception $e) {
                        $logger->info("There was some exception in company save =>" .
                            $e->getMessage());
                        $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Company Save => " .
                            $e->getMessage()];
                    }

                    if (empty($companyId)) {
                        $this->bunchErrorMessage[] =
                            [$row['customer_code'] =>  __("Company Id is mandatory for rest process")];

                        //delete the customer if company not created
                        $this->customerRepository->deleteById($customerId);
                        $logger->info('Associated Customer account is deleted' . $customerId);
                        continue;
                    }
                    $applicablePaymentMethod = $row['applicable_payment_method'];
                    if ($row['available_payment_methods']) {
                        $applicablePaymentMethod = self::APPLICABLE_PAYMENT_METHOD;
                    }
                    $companyPaymentData[] = [
                        'company_id' => $companyId, 'applicable_payment_method' => $applicablePaymentMethod,
                        'available_payment_methods' => $row['available_payment_methods']
                    ];
                    //Add or Update Seller Mapping prepare data
                    $vendorData = $allVendorData[$row['seller_code']];
                    $sellerMappingData = [
                        'retailer_id' => $customerId,
                        'seller_id' => $vendorData['seller_id'],
                        'company_id' => $companyId,
                        'erp_code' => $vendorData['seller_erp_code'],
                        'status' => $statusArray[$row['status']],
                        'seller_code' => $row['seller_code'],
                        'address_id' => $addressId,
                        'customer_group_id' => $defaultCustomerGroupId,
                        'document_type' => '',
                        'cuit_dni_id' => '',
                        'number' => '',
                        'erp_sync_status' => self::ERP_SYNC_STATUS_VALUE,
                        'zone_ids' => $zoneMapping,
                        'email' => $row['email'],
                        'entity_id' => NULL
                    ];
                    if (isset($existingCustomerSellerMapping[$customerId . '_' . $row['seller_code']])) {
                        $sellerMappingData['entity_id'] = $existingCustomerSellerMapping[$customerId . '_' . $row['seller_code']];
                    }
                    $customerSellerMappingInsertData[] = $sellerMappingData;
                } catch (Exception $e) {
                    $logger->info("There was some exception while processing data Final => ." .
                        $e->getMessage());
                    $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Error in complete bunch data processing => " .
                        $e->getMessage()];
                    continue;
                }
                $logger->info('=====Process End for customer code  => ' . $row['customer_code'] . '=====');
            }
            //7. add update company payment details
            if (!empty($companyPaymentData)) {
                $connection = $this->resourceConnection->getConnection();
                $connection->insertOnDuplicate(self::COMPANY_PAYMENT_TABLE, $companyPaymentData);
                unset($companyPaymentData);
            }
            //8. add update customer seller mapping
            if (!empty($customerSellerMappingInsertData)) {
                $connection = $this->resourceConnection->getConnection();
                $connection->insertOnDuplicate(self::UNILEVER_CUSTOMER_SELLER_MAPPING_TABLE, $customerSellerMappingInsertData);
                unset($customerSellerMappingInsertData);
            }
        }
        $this->appEmulation->stopEnvironmentEmulation();
        $logger->info('=====End Processing Total Records  => ' . count($formatedOutletDataWithCustomerId) . '=====');
        $processingResult = $this->bunchErrorMessage;
        return $processingResult;
    }
    /**
     * @param mixed $row
     * @param mixed $customerId
     * @param mixed $isrApproverId
     * @return array
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    private function getCompanyDataFromRow($row, $customerId, $isrApproverId)
    {
        $regOutletId = $this->outletMasterConfig->checkRegOutletId($row['registration_outlet_id']);
        $regSubOutletId = $this->outletMasterConfig->checkRegSubOutletId(
            $regOutletId,
            $row['registration_sub_outlet_id']
        );
        $ulBeatId = $this->outletMasterConfig->checkBeatId($row['ul_beat_id']);
        return [
            'company_name' => $row['company_name'],
            'street' => $row['street'],
            'city' => $row['city'],
            'postcode' => $row['postcode'],
            'country_id' => $row['country_id'],
            'firstname' => $row['firstName'],
            'lastname' => $row['lastName'],
            'region' => $row['region'],
            'region_id' => $this->getRegionId($row['region'], $row['country_id']),
            'district' => $row['district'],
            'customer_group_id' => $this->outletMasterConfig->getDefaultCustomerGroupId(),
            'website_id' => $this->customerRepository->getById($customerId)->getWebsiteId(),
            'super_user_id' => $customerId,
            'vat_tax_id' => $row['vat_tax_id'],
            'geo_coordinates' => $row['geo_coordinates'],
            'customer_code' => $row['customer_code'],
            'registration_outlet_id' => $regOutletId,
            'registration_sub_outlet_id' => $regSubOutletId,
            'seller_code' => $row['seller_code'],
            'distributer_code' => $row['seller_code'],
            'ul_beat_id' => $ulBeatId,
            'whitespace_outlet_status' => $row["whitespace_outlet_status"],
            'delivery_priority' => $row['delivery_priority'],
            'b2b_customer_flag' => $row['b2b_customer'],
            'tenant_code' => $row['tenant_code'] ?? '',
            'approver_id' => $isrApproverId,
            'category_code_1' => $this->outletMasterConfig->getCustomerCategoryCode(
                1,
                $row['category_code_1']
            ) ?? "",
            'category_code_2' => $this->outletMasterConfig->getCustomerCategoryCode(
                2,
                $row['category_code_2']
            ) ?? "",
            'category_code_3' => $this->outletMasterConfig->getCustomerCategoryCode(
                3,
                $row['category_code_3']
            ) ?? "",
            'category_code_4' => $this->outletMasterConfig->getCustomerCategoryCode(
                4,
                $row['category_code_4']
            ) ?? "",
            'category_code_5' => $this->outletMasterConfig->getCustomerCategoryCode(
                5,
                $row['category_code_5']
            ) ?? "",
            'category_code_6' => $this->outletMasterConfig->getCustomerCategoryCode(
                6,
                $row['category_code_6']
            ) ?? "",
            'category_code_7' => $this->outletMasterConfig->getCustomerCategoryCode(
                7,
                $row['category_code_7']
            ) ?? "",
        ];
    }

    /**
     * @return array
     */
    private function getStatusArray()
    {
        return [
            "0" => CompanyStatusInterface::STATUS_PENDING,
            "1" => CompanyStatusInterface::STATUS_APPROVED,
            "2" => CompanyStatusInterface::STATUS_BLOCKED
        ];
    }

    /**
     * @return array
     */
    private function getUlActivationStatusArray()
    {
        return [
            "0" => ActivationStatus::STATUS_PENDING_ACTIVATION,
            "1" => ActivationStatus::STATUS_ACTIVATED,
            "2" => ActivationStatus::STATUS_BLACKLISTED
        ];
    }

    /**
     * @return array
     */
    private function getAddressStatusArray()
    {
        return [
            "0" => CompanyStatusInterface::STATUS_PENDING,
            "1" => ActivationStatus::STATUS_ACTIVATED,
            "2" => CompanyStatusInterface::STATUS_REJECTED
        ];
    }

    /**
     * Get Region ID
     *
     * @param string $stateCode
     * @param int $countryId
     * @return mixed
     */
    public function getRegionId($stateCode, $countryId)
    {
        return $this->regionFactory->create()->loadByCode($stateCode, $countryId)->getRegionId();
    }

    /**
     * Update Mobile Number
     *
     * @param $mobileNumber
     * @return mixed
     */
    public function updateMobileNumber($mobileNumber)
    {
        $updatedMobileNumber = $mobileNumber;
        if ($updatedMobileNumber == '0') {
            $updatedMobileNumber = 'TBD';
        }
        if (!empty($mobileNumber) && $mobileNumber != 'TBD') {
            $fChar = substr($mobileNumber, 0, 1);
            if ($fChar != 0) {
                $updatedMobileNumber = '0' . $mobileNumber;
            }
        }
        return $updatedMobileNumber;
    }
}
