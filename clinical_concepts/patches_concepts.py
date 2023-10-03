# Databricks notebook source
# MAGIC %md
# MAGIC ### Conditions with codes needed to be removed

# COMMAND ----------

autoimmune_disease_noSjorgren_patch_codes = [
  '196137000', 
  '239915006', 
  '762303003', 
  '126766000', 
  '83901003', 
  '78946008', 
  '724782005', 
  '239912009'
]

# COMMAND ----------

chronic_kidney_disease_T2D_Hyp_patch_codes = [
  '118781000119108', #Pre-existing hypertensive chronic kidney disease in mother complicating pregnancy (disorder)
  '421893009', #Renal disorder due to type 1 diabetes mellitus (disorder)
  '722149000', #Chronic kidney disease due to and following excision of neoplasm of kidney (disorder) 
  '722150000', #Chronic kidney disease due to systemic infection (disorder) 
  '722467000', #Chronic kidney disease due to traumatic loss of kidney (disorder) 
  '96441000119101', #Chronic kidney disease due to type 1 diabetes mellitus (disorder) 
  '722098007', #Chronic kidney disease following donor nephrectomy (disorder) 
  '713313000', #Chronic kidney disease mineral and bone disorder (disorder) 
  '18521000119106', #Microalbuminuria due to type 1 diabetes mellitus (disorder)
  '421893009', #Renal disorder due to type 1 diabetes mellitus (disorder) 
  '243421000119104' #Proteinuria due to type 1 diabetes mellitus (disorder)
]

# COMMAND ----------

kidney_cancer_excluding_secondary_malignancy_patch_codes = [
  '94360002 ' # Secondary malignant neoplasm of kidney (disorder)
]

# COMMAND ----------

liver_cancer_patch_codes = [
  '94381002' # Secondary malignant neoplasm of liver (disorder)
]


# COMMAND ----------

malignant_neoplastic_disease_noBSC_noSCC_patch_codes = [
  '429114002', # Malignant basal cell neoplasm of skin (disorder)
  '254651007' #Squamous cell carcinoma of skin (disorder) 
]

# COMMAND ----------

psoriasis_patch_codes = [
  '200956002',            # Psoriatic arthritis with spine involvement
  '156370009',            # Psoriatic arthritis
  '239812005',            # Psoriatic arthritis with distal interphalangeal joint involvement
  '239803008',            # Juvenile psoriatic arthritis with psoriasis 
  '10629311000119107',    # Psoriatic arthritis mutilans
  '239802003',            # Juvenile psoriatic arthritis
  '239804002',            # Juvenile psoriatic arthritis without psoriasis
  '410482007',            # Iritis with psoriatic arthritis
  '239813000',            # Psoriatic dactylitis
  '702617007',            # Acute generalized exanthematous pustulosis
  '33339001',             # Psoriasis with arthropathy
  '19514005',             # Arthritis mutilans
  '65539006',             # Impetigo herpetiformis
  '239098009'            # Acropustulosis of infancy
]

# COMMAND ----------

renal_impairment_no_AKI_patch_codes = [
  '14669001', #Acute renal failure syndrome (disorder)
  '236424009', #Acute renal impairment (disorder)
  '58574008', #Acute nephropathy (disorder)
  '35455006', #Acute tubular necrosis (disorder)
  '429224003', #Acute renal failure due to acute cortical necrosis (disorder) 
  '1144960002', #Acute tubular necrosis due to ischemia and caused by toxin (disorder) 
  '789660001', #Atypical hemolytic uremic syndrome (disorder) 
  '1177174007', #Acute kidney injury following administration of contrast media (disorder)
  '722096006', #Acute kidney injury due to hypovolemia (disorder) 
  '1177174007', #Acute kidney injury following administration of contrast media (disorder)
  '368951000119105', #Acute kidney injury caused by contrast agent (disorder)
  '722095005', #Acute kidney injury due to circulatory failure (disorder)
  '35455006', #Acute tubular necrosis (disorder)
  '444794000', #Acute necrosis of cortex of kidney (disorder)
  '32801008', #Acute pyelitis (disorder)
  '3999002', #Acute pyelitis without renal medullary necrosis (disorder)
  '197770008' #Acute pyonephrosis (disorder)
  ]

# COMMAND ----------

spondyloarthritis_patch_codes = [
  '200956002', #Psoriatic arthritis with spine involvement
  '234524009', #Sarcoid dactylitis
  '201776007', #Rheumatoid arthritis of sacroiliac joint
  '870281008', #Spondyloarthritis caused by parasite
  '239813000', #Psoriatic dactylitis
  '1073381000119103', #Arthritis of left sacroiliac joint caused by bacteria
  '710813008', #Dactylitis of toe
  '49807009', #Tuberculous dactylitis 
  '239812005', #Psoriatic arthritis with distal interphalangeal joint involvement 
  '431236003', #Bacterial arthritis of sacroiliac joint
  '371104006', #Hand-foot syndrome in sickle cell anemia
  '156370009', #Psoriatic arthritis
  '410801005', #Juvenile idiopathic arthritis, enthesitis related arthritis
  '958011000000103',
  '957961000000103',
  '958031000000106',
  '402339007', #Nail dystrophy co-occurrent with reactive arthritis triad 
  '162930007',
  '1073391000119100', #Arthritis of right sacroiliac joint caused by bacteria
  '312411000119100', #Lateral epicondylitis of left humerus 
  '328211000119107', #Dactylitis of finger
  '957971000000105',
  '53286005', #Medial epicondylitis of elbow joint
  '264516005', #Dactylitis
  '313551000119109', #Medial epicondylitis of right humerus 
  '202855006', #Lateral epicondylitis
  '957981000000107',
  '15636001000119108', #Lateral epicondylitis of bilateral humerus  
  '958021000000109',
  '359643005', #Enthesitis (not found?)
  '402338004', #Circinate vulvovaginitis co-occurrent with reactive arthritis triad
  '312421000119107', #Lateral epicondylitis of right humerus
  '73583000', #Epicondylitis
  '957991000000109',
  '958001000000100',
  '957911000000100',
  '957941000000104',
  '957921000000106',
  '957931000000108',
  '15690361000119104', #Arthritis of bilateral sacroiliac joints caused by bacteria
  '238407005', #Blistering distal dactylitis 
  '957951000000101',
  '162930007', #On examination - ankylosing spondylitis chest deformity
  '10317009',  #Spinal enthesopathy
  '1153410003', #Spondyloarthritis caused by bacterium
  '1153411004', #Spondyloarthritis caused by fungus
  '783711009' #Sexually acquired reactive arthritis  
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medications with codes needed to be removed

# COMMAND ----------

calcineurin_inhibitor_patch_codes = [
  '1431983', #24 HR tacrolimus 1 MG Extended Release Oral Capsule
  '2055020', #cycloSPORINE Ophthalmic Solution
  '2055025', #cycloSPORINE, Cequa Ophthalmic Product
  '1732368', #not found
  '1431988', #24 HR tacrolimus 5 MG Extended Release Oral Capsule
  '1093174', #cycloSPORINE 0.002 MG/MG Ophthalmic Ointment
  '2055024', #cycloSPORINE Ophthalmic Solution [Cequa]
  '402330', #cycloSPORINE Ophthalmic Suspension [Restasis]
  '1664465', #24 HR tacrolimus 1 MG Extended Release Oral Tablet [Envarsus]
  '2055021', #cycloSPORINE 0.9 MG/ML Ophthalmic Solution
  '379082', #tacrolimus Topical Ointment
  '1664442', #24 HR tacrolimus 4 MG Extended Release Oral Tablet [Envarsus]
  '1664459', #24 HR tacrolimus 0.75 MG Extended Release Oral Tablet
  '1732369', #5 ML SandIMMUNE 50 MG/ML Injection
  '351291', #cycloSPORINE 0.5 MG/ML Ophthalmic Suspension
  '1664460', #24 HR Envarsus 0.75 MG Extended Release Oral Tablet
  '284520', #Protopic 0.1 % Topical Ointment
  '313189', #tacrolimus 0.0003 MG/MG Topical Ointment
  '367339', #tacrolimus Topical Ointment [Protopic]
  '1664464', #24 HR tacrolimus 1 MG Extended Release Oral Tablet
  '1664441', #24 HR tacrolimus 4 MG Extended Release Oral Tablet
  '1093178', #Optimmune 0.2 % Ophthalmic Ointment
  '1093173', #cycloSPORINE Ophthalmic Ointment
  '1093177', #cycloSPORINE Ophthalmic Ointment [Optimmune]
  '1182527', #Restasis Ophthalmic Product 
  '284521', #Protopic 0.03 % Topical Ointment
  '1738339', #1 ML tacrolimus 5 MG/ML Injection [Prograf]
  '402092', #Restasis 0.05 % Ophthalmic Emulsion
  '1182342', #Optimmune Ophthalmic Produc
  '1156187', #cycloSPORINE Ophthalmic Product
  '1738338', #1 ML tacrolimus 5 MG/ML Injection
  '2055026', #Cequa 0.09 % Ophthalmic Solution
  '1182458', #Protopic Topical Product
  '1431978', #24 HR tacrolimus 0.5 MG Extended Release Oral Capsule
  '1431984', #24 HR tacrolimus 1 MG Extended Release Oral Capsule [Astagraf]
  '314266', #tacrolimus 0.001 MG/MG Topical Ointment
  '1431989', #24 HR Astagraf 5 MG Extended Release Oral Capsule
  '1431979', #24 HR Astagraf 0.5 MG Extended Release Oral Capsule
  '1164204', #tacrolimus Topical Product
  '378766' #cycloSPORINE Ophthalmic Suspension
]

# COMMAND ----------

patches_cc_registry = {
  ## Conditions
  'autoimmune_disease_noSjorgren': autoimmune_disease_noSjorgren_patch_codes,
  'chronic_kidney_disease_T2D_Hyp': chronic_kidney_disease_T2D_Hyp_patch_codes,
  'kidney_cancer_excluding_secondary_malignancy': kidney_cancer_excluding_secondary_malignancy_patch_codes,
  'liver_cancer': liver_cancer_patch_codes,
  'malignant_neoplastic_disease_noBSC_noSCC': malignant_neoplastic_disease_noBSC_noSCC_patch_codes,
  'psoriasis': psoriasis_patch_codes,
  'renal_impairment_no_AKI': renal_impairment_no_AKI_patch_codes,
  'spondyloarthritis': spondyloarthritis_patch_codes,
  ## Medications
  'Calcineurin_inhibitor': calcineurin_inhibitor_patch_codes
}