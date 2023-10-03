# Databricks notebook source
# DBTITLE 1,Define condition label to SNOMED list map
cc_condition_snomed_dictionary = {
  'cystic_fibrosis': ['190905008'],
  'ehlers_danlos_syndrome': ['398114001', '30652003'],
  'diabetes_mellitus_type_2': ['44054006', '422014003'],
  'chronic_heart_failure': ['48447003'],
  'chronic_hypertension': ['59621000', '31992008'],
  'osteoarthritis': ['396275006'],
  'chronic_obstructive_pulmonary_disease': ['13645005'],
  'chronic_ischemic_heart_disease': ['413838009'],
  'chronic_kidney_disease': ['709044004'],
  'osteoporosis': ['64859006'],
  'hyperlipidemia': ['55822004'],
  'atrial_fibrillation': ['49436004'],
  'chronic_hepatitis_b_c': ['128302006', '61977001'],
  'obesity_disorder': ['414916001'],
  'hiv': ['86406008'],
  'psoriatic_arthritis': [
  ## New codes based on Wanessa's work here: https://docs.google.com/spreadsheets/d/1ZguapkDsJTRbP9xZSiWc4OK3T33sUCCCkMfUg20Wgj0/edit#gid=1077580739
  '156370009',    # Psoriatic arthritis (disorder)
  '410482007',    # Iritis with psoriatic arthritis
  '239802003',    # Juvenile psoriatic arthritis (disorder)
  '239803008',    # Juvenile psoriatic arthritis with psoriasis (disorder)
  '1162306005',   # Oligoarticular psoriatic arthritis
  '10629311000119107', # Psoriatic arthritis mutilans (disorder)
  '1162295001', # Psoriatic arthritis of multiple joints (disorder)
  '239812005',  # Psoriatic arthritis with distal interphalangeal joint involvement
  '200956002',  # Psoriatic arthritis with spine involvement
  '239813000',  # Psoriatic dactylitis
  '19514005',   # Arthritis mutilans
  '33339001',   # Psoriasis with arthropathy
  ],
  'psoriasis': [
  '9014002',     # Psoriasis (disorder)
  '400069004', # Nevoid psoriasis (disorder)
  '111188005', # Psoriasiform napkin eruption (disorder)
  ],
  'rheumatoid_arthritis': [
  '69896004',      # Rheumatoid arthritis (disorder)
  '410502007', # Juvenile rheumatoid arthritis or Juvenile idiopathic arthritis (disorder)
  '201796004', # Systemic onset juvenile rheumatoid arthritis or Systemic onset juvenile chronic arthritis (disorder)
  '1162303002', # Seronegative rheumatoid arthritis with erosion of joint (disorder)
  '129563009', # Rheumatoid arthritis with osteoperiostitis (disorder)
  '193180002', # Polyneuropathy in rheumatoid arthritis (disorder)
  '193250002', # Myopathy due to rheumatoid arthritis (disorder)
  '38877003', # Rheumatoid arthritis with aortitis (disorder)
  '400054000', # Rheumatoid arthritis with vasculitis (disorder)
  '1149219001', # Rheumatoid arthritis with systemic vasculitis (disorder)
  '59165007', # Rheumatoid arthritis with scleritis (disorder)
  '77522006', # Rheumatoid arthritis with episcleritis (disorder)
  ],
  ## Added on 2023/07/10
  'narcolepsy': ['60380001'],
  'fabry_disease': ['16652001',],
  'primary_biliary_cholangitis' : ['31712002'],
  'bethlem_myopathy' : ['718572004'],
  'cerebral_autosomal_dominant_arteriopathy_with_subcortical_infarcts_and_leukoencephalopathy' : ['390936003'],
  'hypermobile_ehlers_danlos_syndrome' : ['30652003'],
  'vascular_ehlers_danlos_syndrome' : ['17025000'],
  'ehlers_danlos_syndrome_kyphoscoliotic_type' : ['718211004'],
  'ehlers_danlos_syndrome_procollagen_proteinase_deficient' : ['55711009'],
  'classical_ehlers_danlos_syndrome' : ['715318006'],
  #'dermatosparaxis_ehlers_danlos_syndrome' : ['123722500'],
  'hereditary_insensitivity_to_pain_with_anhidrosis' : ['62985007'],
  'inclusion_body_myositis' : ['72315009'],
  'ullrich_congenital_muscular_dystrophy' : ['240062007'],
  'fatal_familial_insomnia' : ['83157008'],
  'castleman_disease' : ['207036003'],
  'hemophilia' : ['90935002'],
  'spinal_muscular_atrophy' : ['5262007'],
  'retinal_dystrophy' : ['314407005'],
  'familial_x_linked_hypophosphatemic_vitamin_d_refractory_rickets' : ['82236004'],
  'adrenal_cushing_syndrome' : ['237735008'],
  'pulmonary_hypertensive_arterial_disease' : ['11399002'],
  'brugada_syndrome' : ['418818005'],
  'erythropoietic_protoporphyria' : ['51022005'],
  'guillain_barre_syndrome' : ['40956001'],
  'familial_malignant_melanoma_of_skin' : ['726019003'],
  'tetralogy_of_fallot' : ['86299006'],
  #'scleroderma' : ['267874003'],
  'discordant_ventriculoarterial_connection' : ['204296002'],
  'focal_dystonia' : ['445006008'],
  'marfan_syndrome' : ['19346006'],
  'non_hodgkin_lymphoma' : ['118601006'],
  'retinitis_pigmentosa' : ['28835009'],
  'gelineau_syndrome' : ['60380001'],
  'multiple_myeloma' : ['109989006'],
  'alpha_1_antitrypsin_deficiency' : ['30188007'],
  'congenital_diaphragmatic_hernia' : ['17190001'],
  'juvenile_idiopathic_arthritis' : ['410502007'],
  'neurofibromatosis_type_1' : ['92824003'],
  'congenital_atresia_of_esophagus' : ['26179002'],
  'polycythemia_vera' : ['109992005'],
  'hereditary_motor_and_sensory_neuropathy' : ['398100001'],
  'polycystic_kidney_disease_infantile_type' : ['28770003'],
  'vater_association' : ['27742002'],
  'coffin_lowry_syndrome' : ['15182000'],
  'osler_hemorrhagic_telangiectasia_syndrome' : ['21877004'],
  'dermatitis_herpetiformis' : ['111196000'],
  'congenital_atresia_of_small_intestine' : ['84296002'],
  'congenital_atresia_of_duodenum' : ['51118003'],
  'congenital_aganglionic_megacolon' : ['204739008'],
  '22q11_2_deletion_syndrome' : ['767263007'],
  'hereditary_spherocytosis' : ['55995005'],
  'turner_syndrome' : ['38804009'],
  'melas' : ['39925003'],
  'hyperleucinemia' : ['24013007'],
  'medium_chain_acyl_coenzyme_a_dehydrogenase_deficiency' : ['128596003'],
  'lennox_gastaut_syndrome' : ['230418006'],
  'fragile_x_syndrome' : ['613003'],
  'primary_biliary_cholangitis' : ['31712002'],
  'stickler_syndrome' : ['78675000'],
  'williams_syndrome' : ['63247009'],
  'von_willebrand_disorder' : ['128105004'],
  'gastroschisis' : ['72951007'],
  'microphthalmos' : ['61142002'],
  'congenital_omphalocele' : ['18735004'],
  'sarcoidosis' : ['31541009'],
  ##'mayer_rokitansky_kuster_hauser_syndrome_type_2' : [''],
  'stargardt_disease' : ['70099003'],
  'glioblastoma_multiforme' : ['393563007'],
  'multiple_endocrine_neoplasia_type_1' : ['30664006'],
  'prader_willi_syndrome' : ['89392001'],
  'alopecia_totalis' : ['19754005'],
  'nephroblastoma' : ['302849000'],
  'duane_syndrome' : ['60318001'],
  'neuroblastoma' : ['432328008'],
  'hodgkin_disease' : ['118599009'],
  'parkes_weber_syndrome' : ['234143003'],
  'klippel_trenaunay_syndrome' : ['721105004'],
  'whipple_disease' : ['41545003'],
  'incontinentia_pigmenti_syndrome' : ['367520004'],
  'aicardi_syndrome' : ['80651009'],
  'white_matter_disorder_with_cadasil' : ['724779000'],
  'li_fraumeni_syndrome' : ['428850001'],
  'russell_silver_syndrome' : ['15069006'],
  'castleman_disease' : ['207036003'],
  'congenital_livedo_reticularis' : ['254778000'],
  'moebius_syndrome' : ['766987006'],
  'alstrom_syndrome' : ['63702009'],
  'kabuki_make_up_syndrome' : ['313426007'],
  'ondine_curse' : ['399040002'],
  'job_syndrome' : ['50926003'],
  'kearns_sayre_syndrome' : ['25792000'],
  'cholestanol_storage_disease' : ['63246000'],
  'cockayne_syndrome' : ['21086008'],
  'congenital_erythropoietic_porphyria' : ['22935002'],
  'cogan_syndrome' : ['405810005'],
  'kimura_disease' : [ '399894006'],
  'alcohol_abuse': ['15167005'],
  'alcoholic_liver_disease': ['41309000'],
  'allergic_rhinitis': ['61582004'],
  'alzheimers_disease': ['26929004'],
  'ankylosing_spondyloarthritis': ['723116002'],
  'arteriosclerosis': ['441574008'],
  'asthma': ['195967001'],
  'atrial_fibrillation': ['49436004'],
  'attention_deficit_hyperactivity_disorder': ['406506008'],
  'autoimmune_liver_disease': ['235890007'],
  'carotid_atherosclerosis': ['300920004'],
  'celiac_disease': ['396331005'],
  'cerebral_atherosclerosis': ['55382008'],
  'chronic_depression': ['192080009'],
  'chronic_fatigue_syndrome': ['52702003'],
  'chronic_heart_failure': ['48447003'],
  'chronic_hepatitis_b': ['61977001'],
  'chronic_hepatitis_c': ['128302006'],
  #'chronic_kidney_disease_i_to_iv': ['709044004'],
  'chronic_kidney_disease_v': ['433146000', '46177005'],
  'complex_regional_pain_syndrome_': ['128200000'],
  'chronic_obstructive_lung_disease': ['13645005'],
  'crohns_disease': ['34000006'],
  'diabetes_type_1': ['46635009'],
  'diabetes_type_2': ['44054006'],
  'endometriosis_': ['129103003'],
  'eczema': ['43116000'],
  'fibromyalgia_': ['203082005'],
  'gerd': ['235595009'],
  'gout': ['770924008'],
  'graves_disease': ['353295004'],
  'heart_failure': ['84114007'],
  'heart_valve_disease': ['368009'],
  'herpes_simplex': ['88594005'],
  #  'history_of_cerebrovascular_accident': ['30690007', '16896891000119106'],
  #  'history_of_ischemic_heart_disease': ['414545008', '308068007'],
  'hiv': ['19030005'],
  'huntingtons_disease': ['58756001'],
  'hyperglycemia': ['80394007'],
  'hyperlipidemia': ['55822004'],
  'hyperthyroidism': ['34486009'],
  'menieres_disease': ['91011001'],
  'multiple_sclerosis': ['24700007'],
  'narcolepsy': ['60380001'],
  'nonalcoholic_steatohepatitis': ['442685003'],
  'obesity': ['414916001'],
  'obsessive_compulsive_disorder': ['191736004'],
  #  'opiod_abuse': ['560200'],
  'osteoarthritis': ['396275006'],
  'osteoporosis': ['64859006'],
  'parkinsons_disease': ['49049000'],
  'peripheral_arterial_disease': ['840580004'],
  'peripheral_neuropathy': ['302226006'],
  'polycystic_ovarian_disease': ['237055002'],
  'posttraumatic_stress_disorder': ['47505003'],
  'psoriasis_without_psoriatic_arthritis': ['9014002'],
  'psoriatic_arthritis': ['156370009'],
  'pulmonary_hypertension': ['70995007'],
  'rheumatoid_arthritis': ['69896004'],
  'rosacea': ['398909004'],
  'sarcoidosis': ['31541009'],
  'scoliosis': ['298382003'],
  'systemic_lupus_erythematosus': ['200936003'],
  'trigeminal_neuralgia': ['31681005'],
  'ulcerative_colitis': ['64766004'],
  'vascular_dementia': ['429998004'],
  'vasculitis': ['724599009',
              '82275008',
              '310701003',
              '30911005',
              '718217000',
              '195353004',
              '191306005',
              '1144805008',
              '155441006',
              '75053002',
              '2403007',
              '230732009',
              '239938009',
              '239939001',
              '722020006',
              '359789008'],
  'vitamin_b12_deficiency': ['190634004'],
}

# COMMAND ----------

# condition_list = list(cc_condition_snomed_dictionary.keys())
# print(condition_list)
# print(len(condition_list))

# COMMAND ----------

# DBTITLE 1,Track excluded SNOMED codes
cc_condition_snomed_exclusion_list = [
  '305058001',        # Patient encounter status (finding)
  '122541000119104',  # Vaccination needed (situation)
  '24623002',         # Screening mammography (procedure)
  '723620004',        # Requires vaccination (finding)
  '162673000',        # General examination of patient (procedure)
  '19585003',         # Postoperative state (finding)
  '185903001',        # Needs influenza immunization (finding)
  '129825007',        # Healthcare supervision finding (finding)
  '410620009',        # Well child visit (procedure)
  '395170001',        # Medication monitoring (regime/therapy)
  '30058000',         # Therapeutic drug monitoring assay (regime/therapy)
  '169443000',        # Preventive procedure (procedure)
  '72077002',         # Preoperative state (finding)
  '133858001',        # Preoperative procedure (procedure)
  '308273005',        # Follow-up status (finding)
  '309298003',        # Drug therapy finding (finding)
  '309406008',        # Viral screening status (finding)
  '703337007',        # Requires diphtheria, tetanus and pertussis vaccination (finding)
  '183644000',        # Surgical follow-up (finding)
  '310577003',        # Pneumococcal immunization status (finding)
  '243788004',        # Child examination (procedure)
  '243120004',        # Regimes and therapies (regime/therapy)
  '457381000124104',  # Preprocedural examination done (situation)
  '442756004',        # Measurement finding above reference range (finding)
  '2704003',          # Acute disease (disorder)
  '171112000',        # Screening due (finding)
  '268565007',        # Adult health examination (procedure)
  '83607001',         # Gynecologic examination (procedure)
  '441509002',        # Cardiac pacemaker in situ (finding)
  '266713003',        # Long-term drug therapy (procedure)
  '171149006',        # Screening for malignant neoplasm of cervix (procedure)
  '365767001',        # Elevated liver enzymes level (finding)
  '62014003',         # Adverse reaction caused by drug (disorder)
  '183655000',        # Transplant follow-up (finding)
  '442686002',        # Measurement finding below reference range (finding)
  '165325009',        # Biopsy result abnormal (finding)
  '438553004',        # History of drug therapy (situation)
  '58184002',         # Recurrent disease (disorder)
  '170541005',        # Requires a hepatitis A vaccination (finding)
  '789035002',        # Requires vaccination against viral hepatitis (finding)
  '170542003',        # Requires course of hepatitis B vaccination (finding)
  '118188004',        # Finding of neonate (finding)
  '423661009',        # Complication of chemotherapy (disorder)
  '396456003',        # Administration of vaccine product containing only acellular Bordetella pertussis and Corynebacterium diphtheriae and Hepatitis B virus and inactivated whole Human poliovirus antigens (procedure)
  '271903000',        # History of pregnancy (situation)
  '165346000',        # Laboratory test result abnormal (situation)
  '243877001',        # Cancer cervix screening status (finding)
  '268552003',        # Hyperlipidemia screening (procedure)
  '78318003',         # History and physical examination, annual for health maintenance (procedure)
  '409063005',        # Counseling (procedure)
  '429087003',        # History of malignant neoplasm of breast (situation)
  '27624003',         # Chronic disease (disorder)
  '118937003',        # Disorder of lower extremity (disorder)
  '128926000',        # Postprocedural state finding (finding)
  '27624003',         # Chronic disease (disorder)
  '280439003',        # Joint structure of lower leg or tarsus
  '397578001',        # Device in situ (finding)
  '116312005',        # Finding of lower limb (finding)
  '168501001',        # Radiology result abnormal (finding)
  '160433007',        # Family history with explicit context pertaining to father (situation)
  '160427003',        # Family history with explicit context pertaining to mother (situation)
  '87858002',         # Drug-related disorder (disorder)
  '8390008',          # Routine care of newborn (regime/therapy)
  '315215002',        # Disorder excluded (situation)
  '18185001',         # Finding related to pregnancy (finding)
]

# COMMAND ----------

# DBTITLE 1,Define condition feature definitions for Translator
translator_condition_feature_definitions = {
    "22q11_2_deletion_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "22q11_2_deletion_syndrome"
        ],
        "id": "MONDO:0008644",
        "name": "22q11 2 deletion syndrome"
    },
    "adrenal_cushing_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "adrenal_cushing_syndrome"
        ],
        "id": "MONDO:0006640",
        "name": "adrenal cushing syndrome"
    },
    "aicardi_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "aicardi_syndrome"
        ],
        "id": "MONDO:0010568",
        "name": "aicardi syndrome"
    },
    "alcohol_abuse": {
        "category": "biolink:Disease",
        "cc_list": [
            "alcohol_abuse"
        ],
        "id": "MONDO:0002046",
        "name": "alcohol abuse"
    },
    "alcoholic_liver_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "alcoholic_liver_disease"
        ],
        "id": "MONDO:0043693",
        "name": "alcoholic liver disease"
    },
    "allergic_rhinitis": {
        "category": "biolink:Disease",
        "cc_list": [
            "allergic_rhinitis"
        ],
        "id": "MONDO:0011786",
        "name": "allergic rhinitis"
    },
    "alopecia_totalis": {
        "category": "biolink:Disease",
        "cc_list": [
            "alopecia_totalis"
        ],
        "id": "MONDO:0019080",
        "name": "alopecia totalis"
    },
    "alpha_1_antitrypsin_deficiency": {
        "category": "biolink:Disease",
        "cc_list": [
            "alpha_1_antitrypsin_deficiency"
        ],
        "id": "MONDO:0013282",
        "name": "alpha 1 antitrypsin deficiency"
    },
    "alstrom_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "alstrom_syndrome"
        ],
        "id": "MONDO:0008763",
        "name": "alstrom syndrome"
    },
    "alzheimers_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "alzheimers_disease"
        ],
        "id": "MONDO:0004975",
        "name": "alzheimers disease"
    },
    "ankylosing_spondyloarthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "ankylosing_spondyloarthritis"
        ],
        "id": "MONDO:0005306",
        "name": "ankylosing spondyloarthritis"
    },
    "arteriosclerosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "arteriosclerosis"
        ],
        "id": "MONDO:0002277",
        "name": "arteriosclerosis"
    },
    "asthma": {
        "category": "biolink:Disease",
        "cc_list": [
            "asthma"
        ],
        "id": "MONDO:0004979",
        "name": "asthma"
    },
    "atrial_fibrillation": {
        "category": "biolink:Disease",
        "cc_list": [
            "atrial_fibrillation"
        ],
        "id": "MONDO:0004981",
        "name": "atrial fibrillation"
    },
    "attention_deficit_hyperactivity_disorder": {
        "category": "biolink:Disease",
        "cc_list": [
            "attention_deficit_hyperactivity_disorder"
        ],
        "id": "MONDO:0005302",
        "name": "attention deficit hyperactivity disorder"
    },
    "autoimmune_liver_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "autoimmune_liver_disease"
        ],
        "id": "MONDO:0016264",
        "name": "autoimmune liver disease"
    },
    "bethlem_myopathy": {
        "category": "biolink:Disease",
        "cc_list": [
            "bethlem_myopathy"
        ],
        "id": "MONDO:0008029",
        "name": "bethlem myopathy"
    },
    "brugada_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "brugada_syndrome"
        ],
        "id": "MONDO:0015263",
        "name": "brugada syndrome"
    },
    "carotid_atherosclerosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "carotid_atherosclerosis"
        ],
        "id": "UMLS:C0577631",
        "name": "carotid atherosclerosis"
    },
    "castleman_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "castleman_disease"
        ],
        "id": "MONDO:0015564",
        "name": "castleman disease"
    },
    "celiac_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "celiac_disease"
        ],
        "id": "MONDO:0005130",
        "name": "celiac disease"
    },
    "cerebral_atherosclerosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "cerebral_atherosclerosis"
        ],
        "id": "MONDO:0006694",
        "name": "cerebral atherosclerosis"
    },
    "cerebral_autosomal_dominant_arteriopathy_with_subcortical_infarcts_and_leukoencephalopathy": {
        "category": "biolink:Disease",
        "cc_list": [
            "cerebral_autosomal_dominant_arteriopathy_with_subcortical_infarcts_and_leukoencephalopathy"
        ],
        "id": "MONDO:0000914",
        "name": "cerebral autosomal dominant arteriopathy with subcortical infarcts and leukoencephalopathy"
    },
    "cholestanol_storage_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "cholestanol_storage_disease"
        ],
        "id": "MONDO:0008948",
        "name": "cholestanol storage disease"
    },
    "chronic_depression": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_depression"
        ],
        "id": "UMLS:C0581391",
        "name": "chronic depression"
    },
    "chronic_fatigue_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_fatigue_syndrome"
        ],
        "id": "MONDO:0005404",
        "name": "chronic fatigue syndrome"
    },
    "chronic_heart_failure": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_heart_failure"
        ],
        "id": "MONDO:0005009",
        "name": "chronic heart failure"
    },
    "chronic_hepatitis_b": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_hepatitis_b"
        ],
        "id": "MONDO:0005344",
        "name": "chronic hepatitis b"
    },
    "chronic_hepatitis_b_c": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_hepatitis_b_c"
        ],
        "id": "MONDO:0005344",
        "name": "chronic hepatitis b c"
    },
    "chronic_hepatitis_c": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_hepatitis_c"
        ],
        "id": "HP:0200123",
        "name": "chronic hepatitis c"
    },
    "chronic_hypertension": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_hypertension"
        ],
        "id": "UMLS:C0745114",
        "name": "chronic hypertension"
    },
    "chronic_ischemic_heart_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_ischemic_heart_disease"
        ],
        "id": "MONDO:0005010",
        "name": "chronic ischemic heart disease"
    },
    "chronic_kidney_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_kidney_disease"
        ],
        "id": "MONDO:0005300",
        "name": "chronic kidney disease"
    },
    "chronic_kidney_disease_v": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_kidney_disease_v"
        ],
        "id": "UMLS:C1561642",
        "name": "chronic kidney disease v"
    },
    "chronic_obstructive_lung_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_obstructive_lung_disease"
        ],
        "id": "MONDO:0005002",
        "name": "chronic obstructive lung disease"
    },
    "chronic_obstructive_pulmonary_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "chronic_obstructive_pulmonary_disease"
        ],
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease"
    },
    "classical_ehlers_danlos_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "classical_ehlers_danlos_syndrome"
        ],
        "id": "MONDO:0007522",
        "name": "classical ehlers danlos syndrome"
    },
    "cockayne_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "cockayne_syndrome"
        ],
        "id": "MONDO:0016006",
        "name": "cockayne syndrome"
    },
    "coffin_lowry_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "coffin_lowry_syndrome"
        ],
        "id": "MONDO:0010561",
        "name": "coffin lowry syndrome"
    },
    "cogan_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "cogan_syndrome"
        ],
        "id": "MONDO:0015453",
        "name": "cogan syndrome"
    },
    "complex_regional_pain_syndrome_": {
        "category": "biolink:Disease",
        "cc_list": [
            "complex_regional_pain_syndrome_"
        ],
        "id": "MONDO:0019369",
        "name": "complex regional pain syndrome "
    },
    "congenital_aganglionic_megacolon": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_aganglionic_megacolon"
        ],
        "id": "MONDO:0008738",
        "name": "congenital aganglionic megacolon"
    },
    "congenital_atresia_of_duodenum": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_atresia_of_duodenum"
        ],
        "id": "MONDO:0009126",
        "name": "congenital atresia of duodenum"
    },
    "congenital_atresia_of_esophagus": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_atresia_of_esophagus"
        ],
        "id": "MONDO:0001044",
        "name": "congenital atresia of esophagus"
    },
    "congenital_atresia_of_small_intestine": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_atresia_of_small_intestine"
        ],
        "id": "MONDO:0009476",
        "name": "congenital atresia of small intestine"
    },
    "congenital_diaphragmatic_hernia": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_diaphragmatic_hernia"
        ],
        "id": "MONDO:0005711",
        "name": "congenital diaphragmatic hernia"
    },
    "congenital_erythropoietic_porphyria": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_erythropoietic_porphyria"
        ],
        "id": "MONDO:0009902",
        "name": "congenital erythropoietic porphyria"
    },
    "congenital_livedo_reticularis": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_livedo_reticularis"
        ],
        "id": "MONDO:0009055",
        "name": "congenital livedo reticularis"
    },
    "congenital_omphalocele": {
        "category": "biolink:Disease",
        "cc_list": [
            "congenital_omphalocele"
        ],
        "id": "MONDO:0019015",
        "name": "congenital omphalocele"
    },
    "crohns_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "crohns_disease"
        ],
        "id": "MONDO:0005011",
        "name": "crohns disease"
    },
    "cystic_fibrosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "cystic_fibrosis"
        ],
        "id": "MONDO:0009061",
        "name": "cystic fibrosis"
    },
    "dermatitis_herpetiformis": {
        "category": "biolink:Disease",
        "cc_list": [
            "dermatitis_herpetiformis"
        ],
        "id": "MONDO:0015614",
        "name": "dermatitis herpetiformis"
    },
    "diabetes_mellitus_type_2": {
        "category": "biolink:Disease",
        "cc_list": [
            "diabetes_mellitus_type_2"
        ],
        "id": "MONDO:0005148",
        "name": "diabetes mellitus type 2"
    },
    "diabetes_type_1": {
        "category": "biolink:Disease",
        "cc_list": [
            "diabetes_type_1"
        ],
        "id": "MONDO:0005147",
        "name": "diabetes type 1"
    },
    "diabetes_type_2": {
        "category": "biolink:Disease",
        "cc_list": [
            "diabetes_type_2"
        ],
        "id": "MONDO:0005148",
        "name": "diabetes type 2"
    },
    "discordant_ventriculoarterial_connection": {
        "category": "biolink:Disease",
        "cc_list": [
            "discordant_ventriculoarterial_connection"
        ],
        "id": "MONDO:0000153",
        "name": "discordant ventriculoarterial connection"
    },
    "duane_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "duane_syndrome"
        ],
        "id": "MONDO:0007473",
        "name": "duane syndrome"
    },
    "eczema": {
        "category": "biolink:Disease",
        "cc_list": [
            "eczema"
        ],
        "id": "MONDO:0002406",
        "name": "eczema"
    },
    "ehlers_danlos_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "ehlers_danlos_syndrome"
        ],
        "id": "MONDO:0020066",
        "name": "ehlers danlos syndrome"
    },
    "ehlers_danlos_syndrome_kyphoscoliotic_type": {
        "category": "biolink:Disease",
        "cc_list": [
            "ehlers_danlos_syndrome_kyphoscoliotic_type"
        ],
        "id": "MONDO:0016002",
        "name": "ehlers danlos syndrome kyphoscoliotic type"
    },
    "ehlers_danlos_syndrome_procollagen_proteinase_deficient": {
        "category": "biolink:Disease",
        "cc_list": [
            "ehlers_danlos_syndrome_procollagen_proteinase_deficient"
        ],
        "id": "MONDO:0009161",
        "name": "ehlers danlos syndrome procollagen proteinase deficient"
    },
    "endometriosis_": {
        "category": "biolink:Disease",
        "cc_list": [
            "endometriosis_"
        ],
        "id": "MONDO:0005133",
        "name": "endometriosis "
    },
    "erythropoietic_protoporphyria": {
        "category": "biolink:Disease",
        "cc_list": [
            "erythropoietic_protoporphyria"
        ],
        "id": "MONDO:0001676",
        "name": "erythropoietic protoporphyria"
    },
    "fabry_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "fabry_disease"
        ],
        "id": "MONDO:0010526",
        "name": "fabry disease"
    },
    "familial_malignant_melanoma_of_skin": {
        "category": "biolink:Disease",
        "cc_list": [
            "familial_malignant_melanoma_of_skin"
        ],
        "id": "UMLS:C4511622",
        "name": "familial malignant melanoma of skin"
    },
    "familial_x_linked_hypophosphatemic_vitamin_d_refractory_rickets": {
        "category": "biolink:Disease",
        "cc_list": [
            "familial_x_linked_hypophosphatemic_vitamin_d_refractory_rickets"
        ],
        "id": "MONDO:0010619",
        "name": "familial x linked hypophosphatemic vitamin d refractory rickets"
    },
    "fatal_familial_insomnia": {
        "category": "biolink:Disease",
        "cc_list": [
            "fatal_familial_insomnia"
        ],
        "id": "MONDO:0010808",
        "name": "fatal familial insomnia"
    },
    "fibromyalgia_": {
        "category": "biolink:Disease",
        "cc_list": [
            "fibromyalgia_"
        ],
        "id": "MONDO:0005546",
        "name": "fibromyalgia "
    },
    "focal_dystonia": {
        "category": "biolink:Disease",
        "cc_list": [
            "focal_dystonia"
        ],
        "id": "MONDO:0000477",
        "name": "focal dystonia"
    },
    "fragile_x_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "fragile_x_syndrome"
        ],
        "id": "MONDO:0010383",
        "name": "fragile x syndrome"
    },
    "gastroschisis": {
        "category": "biolink:Disease",
        "cc_list": [
            "gastroschisis"
        ],
        "id": "MONDO:0009264",
        "name": "gastroschisis"
    },
    "gelineau_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "gelineau_syndrome"
        ],
        "id": "MONDO:0016158",
        "name": "gelineau syndrome"
    },
    "gerd": {
        "category": "biolink:Disease",
        "cc_list": [
            "gerd"
        ],
        "id": "MONDO:0007186",
        "name": "gerd"
    },
    "glioblastoma_multiforme": {
        "category": "biolink:Disease",
        "cc_list": [
            "glioblastoma_multiforme"
        ],
        "id": "MONDO:0018177",
        "name": "glioblastoma multiforme"
    },
    "gout": {
        "category": "biolink:Disease",
        "cc_list": [
            "gout"
        ],
        "id": "MONDO:0005393",
        "name": "gout"
    },
    "graves_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "graves_disease"
        ],
        "id": "MONDO:0005364",
        "name": "graves disease"
    },
    "guillain_barre_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "guillain_barre_syndrome"
        ],
        "id": "MONDO:0016218",
        "name": "guillain barre syndrome"
    },
    "heart_failure": {
        "category": "biolink:Disease",
        "cc_list": [
            "heart_failure"
        ],
        "id": "MONDO:0005009",
        "name": "heart failure"
    },
    "heart_valve_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "heart_valve_disease"
        ],
        "id": "MONDO:0002869",
        "name": "heart valve disease"
    },
    "hemophilia": {
        "category": "biolink:Disease",
        "cc_list": [
            "hemophilia"
        ],
        "id": "MONDO:0010602",
        "name": "hemophilia"
    },
    "hereditary_insensitivity_to_pain_with_anhidrosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "hereditary_insensitivity_to_pain_with_anhidrosis"
        ],
        "id": "MONDO:0009746",
        "name": "hereditary insensitivity to pain with anhidrosis"
    },
    "hereditary_motor_and_sensory_neuropathy": {
        "category": "biolink:Disease",
        "cc_list": [
            "hereditary_motor_and_sensory_neuropathy"
        ],
        "id": "MONDO:0002316",
        "name": "hereditary motor and sensory neuropathy"
    },
    "hereditary_spherocytosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "hereditary_spherocytosis"
        ],
        "id": "MONDO:0019350",
        "name": "hereditary spherocytosis"
    },
    "herpes_simplex": {
        "category": "biolink:Disease",
        "cc_list": [
            "herpes_simplex"
        ],
        "id": "MONDO:0004609",
        "name": "herpes simplex"
    },
    "hiv": {
        "category": "biolink:Disease",
        "cc_list": [
            "hiv"
        ],
        "id": "FB:FBgn0001201",
        "name": "hiv"
    },
    "hodgkin_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "hodgkin_disease"
        ],
        "id": "MONDO:0004952",
        "name": "hodgkin disease"
    },
    "huntingtons_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "huntingtons_disease"
        ],
        "id": "MONDO:0007739",
        "name": "huntingtons disease"
    },
    "hyperglycemia": {
        "category": "biolink:Disease",
        "cc_list": [
            "hyperglycemia"
        ],
        "id": "MONDO:0002909",
        "name": "hyperglycemia"
    },
    "hyperleucinemia": {
        "category": "biolink:Disease",
        "cc_list": [
            "hyperleucinemia"
        ],
        "id": "HP:0010911",
        "name": "hyperleucinemia"
    },
    "hyperlipidemia": {
        "category": "biolink:Disease",
        "cc_list": [
            "hyperlipidemia"
        ],
        "id": "MONDO:0021187",
        "name": "hyperlipidemia"
    },
    "hypermobile_ehlers_danlos_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "hypermobile_ehlers_danlos_syndrome"
        ],
        "id": "MONDO:0007523",
        "name": "hypermobile ehlers danlos syndrome"
    },
    "hyperthyroidism": {
        "category": "biolink:Disease",
        "cc_list": [
            "hyperthyroidism"
        ],
        "id": "MONDO:0004425",
        "name": "hyperthyroidism"
    },
    "inclusion_body_myositis": {
        "category": "biolink:Disease",
        "cc_list": [
            "inclusion_body_myositis"
        ],
        "id": "MONDO:0007827",
        "name": "inclusion body myositis"
    },
    "incontinentia_pigmenti_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "incontinentia_pigmenti_syndrome"
        ],
        "id": "MONDO:0010631",
        "name": "incontinentia pigmenti syndrome"
    },
    "job_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "job_syndrome"
        ],
        "id": "MONDO:0007818",
        "name": "job syndrome"
    },
    "juvenile_idiopathic_arthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "juvenile_idiopathic_arthritis"
        ],
        "id": "MONDO:0011429",
        "name": "juvenile idiopathic arthritis"
    },
    "kabuki_make_up_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "kabuki_make_up_syndrome"
        ],
        "id": "MONDO:0007843",
        "name": "kabuki make up syndrome"
    },
    "kearns_sayre_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "kearns_sayre_syndrome"
        ],
        "id": "MONDO:0010787",
        "name": "kearns sayre syndrome"
    },
    "kimura_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "kimura_disease"
        ],
        "id": "MONDO:0018830",
        "name": "kimura disease"
    },
    "klippel_trenaunay_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "klippel_trenaunay_syndrome"
        ],
        "id": "MONDO:0007864",
        "name": "klippel trenaunay syndrome"
    },
    "lennox_gastaut_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "lennox_gastaut_syndrome"
        ],
        "id": "MONDO:0016532",
        "name": "lennox gastaut syndrome"
    },
    "li_fraumeni_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "li_fraumeni_syndrome"
        ],
        "id": "MONDO:0007903",
        "name": "li fraumeni syndrome"
    },
    "marfan_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "marfan_syndrome"
        ],
        "id": "MONDO:0007947",
        "name": "marfan syndrome"
    },
    "medium_chain_acyl_coenzyme_a_dehydrogenase_deficiency": {
        "category": "biolink:Disease",
        "cc_list": [
            "medium_chain_acyl_coenzyme_a_dehydrogenase_deficiency"
        ],
        "id": "MONDO:0008721",
        "name": "medium chain acyl coenzyme a dehydrogenase deficiency"
    },
    "melas": {
        "category": "biolink:Disease",
        "cc_list": [
            "melas"
        ],
        "id": "MONDO:0010789",
        "name": "melas"
    },
    "menieres_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "menieres_disease"
        ],
        "id": "MONDO:0007972",
        "name": "menieres disease"
    },
    "microphthalmos": {
        "category": "biolink:Disease",
        "cc_list": [
            "microphthalmos"
        ],
        "id": "MONDO:0021129",
        "name": "microphthalmos"
    },
    "moebius_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "moebius_syndrome"
        ],
        "id": "MONDO:0008006",
        "name": "moebius syndrome"
    },
    "multiple_endocrine_neoplasia_type_1": {
        "category": "biolink:Disease",
        "cc_list": [
            "multiple_endocrine_neoplasia_type_1"
        ],
        "id": "MONDO:0007540",
        "name": "multiple endocrine neoplasia type 1"
    },
    "multiple_myeloma": {
        "category": "biolink:Disease",
        "cc_list": [
            "multiple_myeloma"
        ],
        "id": "MONDO:0009693",
        "name": "multiple myeloma"
    },
    "multiple_sclerosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "multiple_sclerosis"
        ],
        "id": "MONDO:0005301",
        "name": "multiple sclerosis"
    },
    "narcolepsy": {
        "category": "biolink:Disease",
        "cc_list": [
            "narcolepsy"
        ],
        "id": "MONDO:0021107",
        "name": "narcolepsy"
    },
    "nephroblastoma": {
        "category": "biolink:Disease",
        "cc_list": [
            "nephroblastoma"
        ],
        "id": "MONDO:0006058",
        "name": "nephroblastoma"
    },
    "neuroblastoma": {
        "category": "biolink:Disease",
        "cc_list": [
            "neuroblastoma"
        ],
        "id": "MONDO:0005072",
        "name": "neuroblastoma"
    },
    "neurofibromatosis_type_1": {
        "category": "biolink:Disease",
        "cc_list": [
            "neurofibromatosis_type_1"
        ],
        "id": "MONDO:0018975",
        "name": "neurofibromatosis type 1"
    },
    "non_hodgkin_lymphoma": {
        "category": "biolink:Disease",
        "cc_list": [
            "non_hodgkin_lymphoma"
        ],
        "id": "MONDO:0011508",
        "name": "non hodgkin lymphoma"
    },
    "nonalcoholic_steatohepatitis": {
        "category": "biolink:Disease",
        "cc_list": [
            "nonalcoholic_steatohepatitis"
        ],
        "id": "MONDO:0007027",
        "name": "nonalcoholic steatohepatitis"
    },
    "obesity": {
        "category": "biolink:Disease",
        "cc_list": [
            "obesity"
        ],
        "id": "MONDO:0011122",
        "name": "obesity"
    },
    "obesity_disorder": {
        "category": "biolink:Disease",
        "cc_list": [
            "obesity_disorder"
        ],
        "id": "MONDO:0011122",
        "name": "obesity disorder"
    },
    "obsessive_compulsive_disorder": {
        "category": "biolink:Disease",
        "cc_list": [
            "obsessive_compulsive_disorder"
        ],
        "id": "MONDO:0008114",
        "name": "obsessive compulsive disorder"
    },
    "ondine_curse": {
        "category": "biolink:Disease",
        "cc_list": [
            "ondine_curse"
        ],
        "id": "MONDO:0800026",
        "name": "ondine curse"
    },
    "osler_hemorrhagic_telangiectasia_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "osler_hemorrhagic_telangiectasia_syndrome"
        ],
        "id": "MONDO:0019180",
        "name": "osler hemorrhagic telangiectasia syndrome"
    },
    "osteoarthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "osteoarthritis"
        ],
        "id": "MONDO:0005178",
        "name": "osteoarthritis"
    },
    "osteoporosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "osteoporosis"
        ],
        "id": "MONDO:0005298",
        "name": "osteoporosis"
    },
    "parkes_weber_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "parkes_weber_syndrome"
        ],
        "id": "MONDO:0007864",
        "name": "parkes weber syndrome"
    },
    "parkinsons_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "parkinsons_disease"
        ],
        "id": "MONDO:0005180",
        "name": "parkinsons disease"
    },
    "peripheral_arterial_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "peripheral_arterial_disease"
        ],
        "id": "MONDO:0005294",
        "name": "peripheral arterial disease"
    },
    "peripheral_neuropathy": {
        "category": "biolink:Disease",
        "cc_list": [
            "peripheral_neuropathy"
        ],
        "id": "MONDO:0003620",
        "name": "peripheral neuropathy"
    },
    "polycystic_kidney_disease_infantile_type": {
        "category": "biolink:Disease",
        "cc_list": [
            "polycystic_kidney_disease_infantile_type"
        ],
        "id": "MONDO:0009889",
        "name": "polycystic kidney disease infantile type"
    },
    "polycystic_ovarian_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "polycystic_ovarian_disease"
        ],
        "id": "MONDO:0008487",
        "name": "polycystic ovarian disease"
    },
    "polycythemia_vera": {
        "category": "biolink:Disease",
        "cc_list": [
            "polycythemia_vera"
        ],
        "id": "MONDO:0009891",
        "name": "polycythemia vera"
    },
    "posttraumatic_stress_disorder": {
        "category": "biolink:Disease",
        "cc_list": [
            "posttraumatic_stress_disorder"
        ],
        "id": "MONDO:0005146",
        "name": "posttraumatic stress disorder"
    },
    "prader_willi_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "prader_willi_syndrome"
        ],
        "id": "MONDO:0008300",
        "name": "prader willi syndrome"
    },
    "primary_biliary_cholangitis": {
        "category": "biolink:Disease",
        "cc_list": [
            "primary_biliary_cholangitis"
        ],
        "id": "MONDO:0005388",
        "name": "primary biliary cholangitis"
    },
    "psoriasis": {
        "category": "biolink:Disease",
        "cc_list": [
            "psoriasis"
        ],
        "id": "MONDO:0005083",
        "name": "psoriasis"
    },
    "psoriasis_without_psoriatic_arthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "psoriasis_without_psoriatic_arthritis"
        ],
        "id": "UMLS:C0409674",
        "name": "psoriasis without psoriatic arthritis"
    },
    "psoriatic_arthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "psoriatic_arthritis"
        ],
        "id": "MONDO:0011849",
        "name": "psoriatic arthritis"
    },
    "pulmonary_hypertension": {
        "category": "biolink:Disease",
        "cc_list": [
            "pulmonary_hypertension"
        ],
        "id": "MONDO:0005149",
        "name": "pulmonary hypertension"
    },
    "pulmonary_hypertensive_arterial_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "pulmonary_hypertensive_arterial_disease"
        ],
        "id": "MONDO:0015924",
        "name": "pulmonary hypertensive arterial disease"
    },
    "retinal_dystrophy": {
        "category": "biolink:Disease",
        "cc_list": [
            "retinal_dystrophy"
        ],
        "id": "MONDO:0019118",
        "name": "retinal dystrophy"
    },
    "retinitis_pigmentosa": {
        "category": "biolink:Disease",
        "cc_list": [
            "retinitis_pigmentosa"
        ],
        "id": "MONDO:0008377",
        "name": "retinitis pigmentosa"
    },
    "rheumatoid_arthritis": {
        "category": "biolink:Disease",
        "cc_list": [
            "rheumatoid_arthritis"
        ],
        "id": "MONDO:0008383",
        "name": "rheumatoid arthritis"
    },
    "rosacea": {
        "category": "biolink:Disease",
        "cc_list": [
            "rosacea"
        ],
        "id": "MONDO:0006604",
        "name": "rosacea"
    },
    "russell_silver_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "russell_silver_syndrome"
        ],
        "id": "MONDO:0008394",
        "name": "russell silver syndrome"
    },
    "sarcoidosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "sarcoidosis"
        ],
        "id": "MONDO:0008399",
        "name": "sarcoidosis"
    },
    "scoliosis": {
        "category": "biolink:Disease",
        "cc_list": [
            "scoliosis"
        ],
        "id": "MONDO:0005392",
        "name": "scoliosis"
    },
    "spinal_muscular_atrophy": {
        "category": "biolink:Disease",
        "cc_list": [
            "spinal_muscular_atrophy"
        ],
        "id": "MONDO:0001516",
        "name": "spinal muscular atrophy"
    },
    "stargardt_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "stargardt_disease"
        ],
        "id": "MONDO:0019353",
        "name": "stargardt disease"
    },
    "stickler_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "stickler_syndrome"
        ],
        "id": "MONDO:0007160",
        "name": "stickler syndrome"
    },
    "systemic_lupus_erythematosus": {
        "category": "biolink:Disease",
        "cc_list": [
            "systemic_lupus_erythematosus"
        ],
        "id": "MONDO:0007915",
        "name": "systemic lupus erythematosus"
    },
    "tetralogy_of_fallot": {
        "category": "biolink:Disease",
        "cc_list": [
            "tetralogy_of_fallot"
        ],
        "id": "MONDO:0008542",
        "name": "tetralogy of fallot"
    },
    "trigeminal_neuralgia": {
        "category": "biolink:Disease",
        "cc_list": [
            "trigeminal_neuralgia"
        ],
        "id": "MONDO:0008599",
        "name": "trigeminal neuralgia"
    },
    "turner_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "turner_syndrome"
        ],
        "id": "MONDO:0019499",
        "name": "turner syndrome"
    },
    "ulcerative_colitis": {
        "category": "biolink:Disease",
        "cc_list": [
            "ulcerative_colitis"
        ],
        "id": "MONDO:0005101",
        "name": "ulcerative colitis"
    },
    "ullrich_congenital_muscular_dystrophy": {
        "category": "biolink:Disease",
        "cc_list": [
            "ullrich_congenital_muscular_dystrophy"
        ],
        "id": "MONDO:0000355",
        "name": "ullrich congenital muscular dystrophy"
    },
    "vascular_dementia": {
        "category": "biolink:Disease",
        "cc_list": [
            "vascular_dementia"
        ],
        "id": "MONDO:0004648",
        "name": "vascular dementia"
    },
    "vascular_ehlers_danlos_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "vascular_ehlers_danlos_syndrome"
        ],
        "id": "MONDO:0017314",
        "name": "vascular ehlers danlos syndrome"
    },
    "vasculitis": {
        "category": "biolink:Disease",
        "cc_list": [
            "vasculitis"
        ],
        "id": "MONDO:0018882",
        "name": "vasculitis"
    },
    "vater_association": {
        "category": "biolink:Disease",
        "cc_list": [
            "vater_association"
        ],
        "id": "MONDO:0008642",
        "name": "vater association"
    },
    "vitamin_b12_deficiency": {
        "category": "biolink:Disease",
        "cc_list": [
            "vitamin_b12_deficiency"
        ],
        "id": "MONDO:0000424",
        "name": "vitamin b12 deficiency"
    },
    "von_willebrand_disorder": {
        "category": "biolink:Disease",
        "cc_list": [
            "von_willebrand_disorder"
        ],
        "id": "MONDO:0019565",
        "name": "von willebrand disorder"
    },
    "whipple_disease": {
        "category": "biolink:Disease",
        "cc_list": [
            "whipple_disease"
        ],
        "id": "MONDO:0005116",
        "name": "whipple disease"
    },
    "white_matter_disorder_with_cadasil": {
        "category": "biolink:Disease",
        "cc_list": [
            "white_matter_disorder_with_cadasil"
        ],
        "id": "UMLS:C4511433",
        "name": "white matter disorder with cadasil"
    },
    "williams_syndrome": {
        "category": "biolink:Disease",
        "cc_list": [
            "williams_syndrome"
        ],
        "id": "MONDO:0008678",
        "name": "williams syndrome"
    }
}

# COMMAND ----------

# DBTITLE 1,Sanity check the dictionary, please make sure that those two key list are the same length, and the difference list contains no key
## Sanity check the dictionary, please make sure that those two key list are the same length, and the difference list contains no key
list1 = list(cc_condition_snomed_dictionary.keys())
print(len(list1))

list2 = list(translator_condition_feature_definitions.keys())
print(len(list2))

print("Please sanity check to make sure the following two numbers are identical, otherwise there will be dimension not matched issues.")
print(list1)