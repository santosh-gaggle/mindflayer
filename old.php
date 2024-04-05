<?php

declare(strict_types=1);

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

class ProcessOutletMaster
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
    public const WHITESPCE_STATUS ='whitespace_outlet_status';
    public const TABLE_WHITESPCE = 'unilever_whitespace_outlet';

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
    ) {
        if (!$this->registry->registry('isSecureArea')) {
            $registry->register('isSecureArea', true);
        }
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
    public function processRecords($outletData, $storeId, $isrApproverId, $logger)
    {
        $logger->info('=====Start Processing Total Records  => '.count($outletData).'=====');
        $processingResult = [];
        $store = $this->storeManager->getStore($storeId);
        $statusArray = $this->getStatusArray();
        $ulActivationStatusArray = $this->getUlActivationStatusArray();
        $addressStatusArray = $this->getAddressStatusArray();
        $this->appEmulation->startEnvironmentEmulation($storeId, Area::AREA_FRONTEND, true);
        $this->bunchErrorMessage = [];
        foreach ($outletData as $row) {
            try {
                if (empty($row['customer_code'])) {
                    $logger->info('-----Customer Code is mandatory-----');
                    $this->bunchErrorMessage[] = [$row['company_name'] =>
                        __('Column customer_code (%1) is empty.', $row['customer_code'])];
                    continue;
                }

                $logger->info('=====Process Start for customer code  => ' . $row['customer_code'] . '=====');
                //updating the row telephone
                $row['telephone'] = $this->updateMobileNumber($row['telephone']);
                //replacing first name and last name special characters
                $specialCharString = $this->outletMasterConfig->getSpecialCharacterRegularExpression();
                if (!empty($specialCharString)) {
                    $row['firstName'] = preg_replace($specialCharString, '', $row['firstName']);
                    $row['lastName'] = preg_replace($specialCharString, '', $row['lastName']);
                }
                // setting 0 value if index not set
                if (!isset($row['b2b_customer'])|| $row['b2b_customer'] == null) {
                    $row['b2b_customer'] = 0;
                }

                $logger->info('-----Start Processing-----' . $row['customer_code'] . '-----');
                // check for the whitespace outlet or not
                if ($row['whitespace_outlet_status'] == 1 || $row['whitespace_outlet_status'] == "1") {
                    $logger->info('-----WhiteSpace Outlet----' . $row['customer_code'] . '-----');
                    $this->addWhiteSpaceOutletDetails($row, $storeId);
                } else {
                    $logger->info('-----No WhiteSpace Outlet----' . $row['customer_code'] . '-----');
                    $customerId = '';
                    try {
                        //Create Or Update Customer
                        $logger->info('-----Customer Found Process Started-----');
                        $customerAdditionalData = [];
                        $vendorData = $this->outletMasterConfig->getVendorData($row['seller_code']);
                        $existingCustomerData = $this->outletMasterConfig->getCustomerId(
                            $row['customer_code'],
                            $row['seller_code']
                        );
                        $existingCustomerId = $existingCustomerEmail = '';
                        if (!empty($existingCustomerData) && is_array($existingCustomerData)) {
                            $existingCustomerId = $existingCustomerData[0]['entity_id'];
                            $existingCustomerEmail = $existingCustomerData[0]['email'];
                        }
                        $customerId = (int) $this->createUpdateCustomerData($existingCustomerId, $existingCustomerEmail, $row, $storeId, $vendorData, $logger);
                        if (empty($customerId)) {
                            $this->bunchErrorMessage[] = [$row['customer_code'] => __("Customer Id empty")];
                            continue;
                        }
                        $logger->info('-----Customer ID for the Rest process: ' . $customerId . '-----');
                        $customer = $this->customerRepository->getById($customerId);
                        $customer->setId($customerId);
                        $customer->setCustomAttribute('isr_status', $statusArray[$row['status']]);
                        $customer->setCustomAttribute(
                            'ul_activation_status',
                            $ulActivationStatusArray[$row['status']]
                        );
                        $this->customerRepository->save($customer);
                        $createdAt = $customer->getCreatedAt();
                        if (empty($existingCustomerId)) {
                            $customerAdditionalData['mobile_number'] = $row['telephone'];
                            $customerAdditionalData['customer_code'] = $row['customer_code'];
                            $customerAdditionalData['invite_code'] = $this->outletMasterConfig->getInviteCode($storeId);
                        }
                        if ($row['b2b_customer'] == "1" || $row['b2b_customer'] == 1) {
                            $customerAdditionalData['sync_status'] = self::ERP_SYNC_B2B_OUTLET_FLAG;
                            $customerAdditionalData['invite_code'] = '';
                        }
                        $customerAdditionalData['vendor_id'] = $vendorData['seller_id'];
                        $customerAdditionalData['vendor_group_id'] = $vendorData['seller_group_id'];
                        $customerAdditionalData['zone_mapping'] = $this->outletMasterConfig->getDefaultZone();
                        $where = '(entity_id = "' . $customerId . '")';
                        $this->resourceConnection->getConnection()->update(
                            self::TABLE_CUSTOMER_ENTITY,
                            $customerAdditionalData,
                            $where
                        );
                        $logger->info('-----Customer Attributes Added Successfully => ' .
                            $row['telephone'] . '-----');
                        //Create or update Customer Address

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
                            if ($this->customerRepository->getById($customerId)->getDefaultShipping()) {
                                $addressId = $this->customerRepository->getById($customerId)->getDefaultShipping();
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
                                $creditLimit = $this->creditLimitManagement->getCreditByCompanyId($companyId);
                                $currencyCode = $store->getCurrentCurrencyCode();
                                $creditLimit->setCurrencyCode($currencyCode)->save();
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

                                // $companyData['is_sync_required'] = 1; // comment this for DataSync issue

                                $companyDetails = $this->companyFactory->create();
                                $companyDetails->setData($companyData)->save();
                                $creditLimit = $this->creditLimitManagement->getCreditByCompanyId($companyId);
                                $currencyCode = $store->getCurrentCurrencyCode();
                                $creditLimit->setCurrencyCode($currencyCode)->save();
                                $logger->info('-----Company updated Successfully-----');
                            }
                        } catch (LocalizedException $e) {
                            $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Localized exception on company save => ".
                                $e->getMessage()];
                        } catch (Exception $e) {
                            $logger->info("There was some exception in company save =>" .
                                $e->getMessage());
                            $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Company Save => ".
                                $e->getMessage()];
                        }

                        if (empty($companyId)) {
                            $this->bunchErrorMessage[] =
                                [$row['customer_code'] =>  __("Company Id is mandatory for rest process")];

                            //delete the customer if company not created
                            $this->customerRepository->deleteById($customerId);
                            $logger->info('Associated Customer account is deleted'. $customerId);
                            continue;
                        }
                        $this->addCompanyPaymentDetails($row, $companyId, $logger);
                        //Add or Update Seller Mapping
                        $vendorData = $this->outletMasterConfig->getVendorData($row['seller_code']);
                        try {
                            $sellerMappingData = [
                                'retailer_id' => $customerId,
                                'seller_id' => $vendorData['seller_id'],
                                'company_id' => $companyId,
                                'erp_code' => $vendorData['seller_erp_code'],
                                'status' => $statusArray[$row['status']],
                                'seller_code' => $row['seller_code'],
                                'address_id' => $addressId,
                                'customer_group_id' => $this->outletMasterConfig->getDefaultCustomerGroupId(),
                                'document_type' => '',
                                'cuit_dni_id' => '',
                                'number' => '',
                                'erp_sync_status' => self::ERP_SYNC_STATUS_VALUE,
                                'zone_ids' => $this->outletMasterConfig->getDefaultZone()
                            ];
                            $sellerChecker = [
                                'seller_code' => $row['seller_code'],
                                'customer_id' => $customerId,
                                'company_id' => $companyId
                            ];
                            $sellerMappingModel = $this->distributorZoneMappingRepository->findOrCreateSellerMapping($sellerChecker);
                            if ($sellerMappingModel->getId()) {
                                $sellerMappingData['entity_id'] = $sellerMappingModel->getId();
                            } else {
                                $sellerMappingData['email'] = $row['email'];
                            }
                            $sellerMappingModel->setData($sellerMappingData);
                            $sellerMappingModel->save();
                        } catch (Exception $e) {
                            $logger->info("There was some exception in vendor data save." .
                                $e->getMessage());
                            $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Vendor Association Save => ".
                                $e->getMessage()];
                        }
                        $logger->info('-----Company Vendor Data Success-----');
                    } catch (Exception $e) {
                        $logger->info("There was some exception while processing data => ." .
                            $e->getMessage());
                        $this->bunchErrorMessage[] =
                            [$row['customer_code'] => "Error in full Customer details update => ". $e->getMessage()];
                        continue;
                    }
                }
            } catch (Exception $e) {
                $logger->info("There was some exception while processing data Final => ." .
                    $e->getMessage());
                $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Error in complete bunch data processing => ".
                    $e->getMessage()];
                continue;
            }
            $logger->info('=====Process End for customer code  => ' . $row['customer_code'] . '=====');
        }
        $this->appEmulation->stopEnvironmentEmulation();
        $logger->info('=====End Processing Total Records  => '.count($outletData).'=====');
        $processingResult = $this->bunchErrorMessage;
        return $processingResult;
    }

    /**
     * @param mixed $existingCustomerId
     * @param mixed $existingEmail
     * @param mixed $row
     * @param mixed $storeId
     * @param mixed $vendorData
     * @param object $logger
     * @return int
     * @throws \Magento\Framework\Exception\InputException
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     * @throws \Magento\Framework\Exception\State\InputMismatchException
     */
    private function createUpdateCustomerData($existingCustomerId, $existingEmail, $row, $storeId, $vendorData, $logger): int|string|null
    {
        $logger->info('-----In Create/Update Customer Data-----');
        $store = $this->storeManager->getStore($storeId);
        $websiteId = $store->getWebsiteId();
        $customerId = '';
        if ((!empty($vendorData)) && ($vendorData['seller_id'] && $vendorData['seller_group_id'])) {
            $logger->info('-----Appropriate vendor data exist-----');
            try {
                $newCustomer = $this->customerInterfaceFactory->create();
                if (!empty($existingCustomerId)) {
                    $newCustomer->setWebsiteId($websiteId);
                    $newCustomer->setId($existingCustomerId);
                    $logger->info('-----Customer Loaded with the Id => ' .
                        $newCustomer->getId() . '-----');
                    $logger->info('-----Customer Loaded with the Email => ' .
                        $existingEmail . '-----');
                    $row['email'] = $existingEmail;
                }
                $newCustomer->setWebsiteId($websiteId);
                $newCustomer->setStoreId($storeId);
                $newCustomer->setFirstname($row['firstName']);
                $newCustomer->setLastname($row['lastName']);
                $newCustomer->setEmail($row['email']);
                $newCustomer->setDob($row['dob']);
                $newCustomer->setGroupId($this->outletMasterConfig->getDefaultCustomerGroupId());
                $storeName = $this->storeManager->getStore($newCustomer->getStoreId())->getName();
                $newCustomer->setCreatedIn($storeName);
                $newCustomer->setCustomAttribute('customer_code', $row['customer_code']);
                $this->customerRepository->save($newCustomer);
                //Get the created customer id
                $customerId = $this->customerRepository->get($row['email'], $websiteId)->getId();
                $logger->info('-----Customer Created/Updated Successfully => ' .
                    $row['telephone'] . '-----');
            } catch (Exception $e) {
                $logger->info("Customer Add Error => ." . $e->getMessage());
                $this->bunchErrorMessage[] = isset($row['customer_code'])? [$row['customer_code'] =>
                    __("There is an issue in Customer account Creation.")] : $e->getMessage();
                return $customerId;
            }
        } else {
            $logger->info('-----Not Appropriate vendor Data => ' . $row['telephone'] . '-----');
            $this->bunchErrorMessage[] = [$row['customer_code'] => __('vendor data is not correct.')];
            return $customerId;
        }
        return $customerId;
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
     * @param mixed $row
     * @return void
     */
    private function addWhiteSpaceOutletDetails($row, $storeId)
    {
        $data = [
            self::WHITESPCE_SELLER_CODE => $row['seller_code'],
            self::WHITESPCE_COMPANY_EMAIL => $row['company_email'],
            self::WHITESPCE_TELEPHONE => $row['telephone'],
            self::WHITESPCE_STATUS => $row['whitespace_outlet_status']
        ];
        $where = '(customer_code = "' . $row['customer_code'] . '")';
        $select = $this->resourceConnection->getConnection()->select()
            ->from(self::TABLE_WHITESPCE)
            ->where($where);
        $whiteSpaceData = $this->resourceConnection->getConnection()->fetchAll($select);
        if (empty($whiteSpaceData)) {
            $data[self::WHITESPCE_CUSTOMER_CODE] = $row['customer_code'];
            $data[self::WHITESPCE_INVITE_CODE] = $this->outletMasterConfig->getInviteCode($storeId);
            $this->resourceConnection->getConnection()->insertOnDuplicate(self::TABLE_WHITESPCE, $data);
        } else {
            $this->resourceConnection->getConnection()->update(self::TABLE_WHITESPCE, $data, $where);
        }
    }

    /**
     * @param mixed $row
     * @param mixed $companyId
     * @param object $logger
     * @return void
     */
    private function addCompanyPaymentDetails($row, $companyId, $logger)
    {
        //Add or Update Payment Details to Company
        try {
            $paymentSettings = $this->companyPaymentMethodFactory->create()->load($companyId);
            if (!$paymentSettings->getId()) {
                $paymentSettings->setCompanyId($companyId);
            }
            $applicablePaymentMethod = $row['applicable_payment_method'];
            if ($row['available_payment_methods']) {
                $applicablePaymentMethod = self::APPLICABLE_PAYMENT_METHOD;
            }
            $paymentSettings->setAvailablePaymentMethods($row['available_payment_methods']);
            $paymentSettings->setApplicablePaymentMethod($applicablePaymentMethod);
            $this->companyPaymentMethodResource->save($paymentSettings);
            $logger->info('-----Company Payment saved Successfully-----');
        } catch (Exception $e) {
            $logger->info("There was some exception in payment details save." . $e->getMessage());
            $this->bunchErrorMessage[] =  [$row['customer_code'] =>  "Company Payment Details => ". $e->getMessage()];
        }
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
                $updatedMobileNumber = '0'.$mobileNumber;
            }
        }
        return $updatedMobileNumber;
    }
}
