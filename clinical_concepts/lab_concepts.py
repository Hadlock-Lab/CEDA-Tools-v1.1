# Databricks notebook source
# DBTITLE 1,Define lab field search strings map
cc_lab_search_strings = {
  'adenovirus_pcr_naat': {
    'common_name': [
      'ADENOVIRUS DNA. XXX. ORD. PROBE', 'ADENOVIRUS.XXX.ORD(BEAKER)', 'ADENOVIRUS (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'bordetella_pertussis_pcr_naat': {
    'common_name': ['BORDETELLA PERTUSSIS DNA', 'BORDETELLA PERTUSSIS PCR', 'BORDETELLA PERTUSSIS'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'campylobacter_pcr_naat': {
    'common_name': ['CAMPYLOBACTER NAAT (BEAKER)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'chlamydia_pneumoniae_pcr_naat': {
    'common_name': ['CHLAMYDOPHILA PNEUMONIAE PCR', 'CHLAMYDIA PNEUMONIAE.XXX.ORD(BEAKER)', 'CHLAMYDOPHILA PNEUMONIAE'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'chlamydia_trachomatis_pcr_naat' : {
    'common_name': [
      'C. TRACH AMP RNA', 'CHLAMYDIA TRACHOMATIS DNA PCR', 'CHLAMYDIA NAAT', 'CHLAMYDIA TRACHOMATIS DNA',
      'CHLAMYDIA TRACHOMATIS PCR', 'CHLAMYDIA TRACHOMATIS NAAT', 'CHLAM DNA PB', 'CHLAMYDIA TRACHOMATIS RRNA PCR'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'clostridium_difficile_pcr_naat': {
    'common_name': [
      'C DIFFICILE NAAT', 'TOXIGENIC C DIFFICILE', 'C DIFFICILE ANTIGEN', 'C DIFFICILE TOX AG', 'C DIFFICILE TOX DNA',
      'C. DIFFICILE AG', 'C DIFFICILE TOXIN', 'CLOSTRIDIUM DIFFICILE TOXIN A/B NAAT (BEAKER)',
      'CLOSTRIDIUM DIFFICILE TOXIN A/B NAAT (BEAKER)', 'C DIFFICILE TOXINS A+ B, EIA', 'C DIFFICILE TOXIN A/B',
      'CLOSTRIDIUM DIFFICILE TOXIN A/B NAAT', 'CLOSTRIDIUM DIFFICILE TOXIN B.STOOL.ORD (REF)',
      'CLOSTRIDIUM DIFFICILE TOXIN A/B DNA (BEAKER)', 'C DIFFICILE TOXIN PCR(UW)', 'C DIFFICILE CYTOTOXIN AB (PAML)',
      'CLOSTRIDIUM DIFFICILE TOXIN A/B(REF)', 'CLOSTRIDIUM DIFFICILE NAAT'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'coronavirus_other_pcr_naat': {
    'common_name': [
      'CORONAVIRUS 229E', 'CORONAVIRUS HKU1', 'CORONAVIRUS OC43', 'CORONAVIRUS NL63', 'CORONAVIRUS NL63.XXX.ORD(BEAKER)',
      'CORONAVIRUS HKU1 RNA. XXX. ORD. PROBE', 'CORONAVIRUS OC43 RNA. XXX. ORD. PROBE', 'CORONAVIRUS 229E RNA. XXX. ORD. PROBE',
      'CORONAVIRUS NL63 RNA. XXX. ORD. PROBE', 'CORONAVIRUS 229E. XXX. ORD(BEAKER)', 'CORONAVIRUS HKU1.XXX.ORD(BEAKER)',
      'CORONAVIRUS OC43.XXX.ORD(BEAKER)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'hepatitis_c_pcr_naat': {
    'common_name': ['HEP C RT-PCR'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'herpes_simplex_virus_pcr_naat': {
    'common_name': [
      'HSV/PCR RESULT', 'HSV DNA TYPE 1', 'HSV DNA TYPE 2', 'HSV 1 BY PCR', 'HSV 2 BY PCR', 'HSV PCR COPIES/ML',
      'HSV DNA LOG 10', 'HSV2', 'HSV1', 'HSV TYPE 2', 'HSV-2 DNA PCR, QNT (SOFT)', 'HSV-1 DNA PCR, QNT (SOFT)',
      'HSV BY PCR', 'HSV-1 DNA PCR, QNT', 'HSV-2 DNA PCR, QNT', 'HSV PCR QUANT', 'HSV TYPE 1', 'HSV TYPE 2',
      'HERPES SIMPLES VIRUS 2 (HSV-2) (CSF)', 'HERPES SIMPLES VIRUS 1 (HSV-1) (CSF)', 'HSV2', 'HSV BY RT-PCR QNT',
      'HSV DNA TYPE 1', 'HSV DNA TYPE 2'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'human_immunodeficiency_virus_pcr_naat': {
    'common_name': ['HIV-1 RNA BY PCR, QN'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'human_metapneumovirus_pcr_naat': {
    'common_name': ['METAPNEUMOVIRUS PCR', 'HUMAN METAPNEUMOVIRUS.XXX.ORD(BEAKER)', 'METAPNEUMOVIRUS (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'human_papillomavirus_pcr_naat': {
    'common_name': ['HPV, HIGH-RISK', 'HPV GENOTYPE 16', 'HPV GENOTYPE 18', 'HPV DNA'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'influenza_pcr_naat': {
    'common_name': [
      'INFLUENZA A PCR', 'INFLUENZA B PCR', 'INFLUENZA A PCR(BEAKER)', 'INFLUENZA B PCR (BEAKER)', 'INFLUENZA A NAAT, POC',
      'INFLUENZA B NAAT, POC', 'INFLUENZA A/B RT PCR (SOFT)', 'INFLUENZA VIRUS A H3 RNA', 'INFLUENZA A 2009 H1N1 STRAIN PCR',
      'INFLUENZA A NAAT, POC', 'INFLUENZAE A, B, AND H1N1 2009 PCR (SOFT)', 'INFLUENZA A, RT-PCR', 'INFLUENZA VIRUS A H1 RNA',
      'INFLUENZA A H3.XXX.ORD(BEAKER)', 'INFLUENZA A H1.XXX.ORD(BEAKER)', 'INFLUENZA A 2009 H1.XXX.ORD.BEAKER',
      'INFLUENZA B (RVP)', 'INFLUENZA A (RVP)', 'INFLUENZA A 2009 H1', 'INFLUENZA H1N1'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'influenza_a_pcr_naat': {
    'common_name': [
      'INFLUENZA A PCR', 'INFLUENZA A PCR(BEAKER)', 'INFLUENZA A NAAT, POC', 'INFLUENZA VIRUS A H3 RNA',
      'INFLUENZA A 2009 H1N1 STRAIN PCR', 'INFLUENZA A NAAT, POC', 'INFLUENZA A, RT-PCR', 'INFLUENZA VIRUS A H1 RNA',
      'INFLUENZA A H3.XXX.ORD(BEAKER)', 'INFLUENZA A H1.XXX.ORD(BEAKER)', 'INFLUENZA A 2009 H1.XXX.ORD.BEAKER',
      'INFLUENZA A (RVP)', 'INFLUENZA A 2009 H1'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'influenza_b_pcr_naat': {
    'common_name': [
      'INFLUENZA B PCR', 'INFLUENZA B PCR (BEAKER)', 'INFLUENZA B NAAT, POC', 'INFLUENZA B (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'mrsa_pcr_naat': {
    'common_name': ['MRSA, DNA', 'MRSA PCR'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'mycoplasma_pneumoniae_pcr_naat': {
    'common_name': [
      'MYCOPLAMA PNEUMO PCR', 'MYCOPLASMA PNEUMONIAE.XXX.ORD(BEAKER)', 'M PNEUMONIAE DNA BY PCR'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'neisseria_gonorrhoeae_pcr_naat': {
    'common_name': [
      'NEISSERIA GONORRHOEAE DNA PCR', 'NEISSERIA GONORRHOEAE PCR', 'GC NAAT', 'NEISSERIA GONORRHOEAE DNA',
      'NEISSERIA GONORRHOEAE RRNA PCR', 'GC DNA PROBE', 'GC, EXTERNAL'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'parainfluenza_pcr_naat': {
    'common_name': [
      'PARAINFLUENZA VIRUS 3. XXX. ORD. PROBE', 'PARAINFLUENZA VIRUS 4. XXX. ORD. PROBE', 'PARAINFLUENZA VIRUS 1. XXX. ORD. PROBE',
      'PARAINFLUENZA VIRUS 2. XXX. ORD. PROBE', 'PARAINFLUENZA 3.XXX.ORD(BEAKER)', 'PARAINFLUENZA 1.XXX.ORD(BEAKER)',
      'PARAINFLUENZA 4.XXX.ORD(BEAKER)', 'PARAINFLUENZA 2.XXX.ORD(BEAKER)', 'PARAINFLUENZA 3 (RVP)', 'PARAINFLUENZA 1 (RVP)',
      'PARAINFLUENZA 4', 'PARAINFLUENZA 2 (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'respiratory_syncytial_virus_pcr_naat': {
    'common_name': ['RSV', 'RSV PCR', 'RESPIRATORY SYNCYTIAL VIRUS A', 'RSV B', 'RSV A (RVP)', 'RSV B (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'rhinovirus_enterovirus_pcr_naat': {
    'common_name': [
      'RHINOVIRUS/ENTEROVIRUS', 'RHINOVIRUS+ ENTEROVIRUS. XXX. ORD. PROBE', 'RHINOVIRUS/ENTEROVIRUS.XXX.ORD(BEAKER)', 'RHINOVIRUS BY PCR',
      'RHINOVIRUS (RVP)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'salmonella_pcr_naat': {
    'common_name': ['SALMONELLA NAAT (BEAKER)'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'sars_cov_2_pcr_naat': {
    'common_name': [
      'SARS CORONAVIRUS 2 RNA ORD', 'POC SARS CORONAVIRUS 2 RNA ORD', 'SARS-COV-2 (COVID-19) QUAL PCR RESULT', 'COV19EX'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'shigella_enteroinvasive_e_coli_pcr_naat': {
    'common_name': ['SHIGELLA/ENTEROINVASIVE E. COLI (EIEC) NAAT (BEAKER)', 'SHIGA TOXIN NAAT'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'staphylococcus_aureus_pcr_naat': {
    'common_name': ['STAPH AUREUS BY PCR', 'STAPHYLOCOCCUS AUREUS, DNA'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'streptococcus_group_a_pcr_naat': {
    'common_name': ['GROUP A STREP, ORD DNA, POC', 'STREP PYOGENES, NAAT'],
    'order_name': ['NAAT', 'PCR']
  },
  
  'trichomonas_vaginalis_pcr_naat': {
    'common_name': ['TRICHOMONAS VAGINALIS DNA PROBE'],
    'order_name': ['NAAT', 'PCR']
  },
}

# COMMAND ----------

# DBTITLE 1,Define lab label to LOINC list map
cc_lab_loinc_dictionary = {
  'acetylcholinesterase_presence_amniotic_fluid': [
    '30106-9',   # Acetylcholinesterase [Presence] in Amniotic fluid
  ],
  'albumin': [
    '1751-7',  # Albumin [Mass/volume] in Serum or Plasma
    '61151-7', # Albumin [Mass/volume] in Serum or Plasma by Bromocresol green (BCG) dye binding method
    '61152-5'  # Albumin [Mass/volume] in Serum or Plasma by Bromocresol purple (BCP) dye binding method
  ],
  'albumin_globulin_ratio': [
    '1759-0',  # Albumin/Globulin [Mass Ratio] in Serum or Plasma
    '44429-9'  # Albumin/Globulin [Mass Ratio] in Serum or Plasma by Electrophoresis
  ],
  'alp': [
    '6768-6',   # Alkaline phosphatase [Enzymatic activity/volume] in Serum or Plasma
    '1779-8',   # Alkaline phosphatase.liver [Enzymatic activity/volume] in Serum or Plasma
    '1783-0',   # Alkaline phosphatase [Enzymatic activity/volume] in Blood
  ],
  'alt': [
    '1742-6',  # Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma
    '1744-2',  # Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma by No addition of P-5'-P
    '1743-4'   # Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma by With P-5'-P
  ],
  'anion_gap': [
    '10466-1',  # Anion gap 3 in Serum or Plasma
    '33037-3',  # Anion gap in Serum or Plasma
    '1863-0'    # Anion gap 4 in Serum or Plasma
  ],
  'appearance_of_urine': [
    '5767-9',   # Appearance of Urine
  ],
#   'arbovirus_ab': [
#     '44074-3',   # Arbovirus Ab [Interpretation] in Serum by Immunofluorescence
#   ],
  'ast': [
    '1920-8'   # Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma
  ],
  'bacteria_in_blood': [
    '600-7',    # Bacteria identified in Blood by Culture
  ],
  'bacteria_in_body_fluid': [
    '610-6',    # Bacteria identified in Body fluid by Aerobe culture
  ],
  'bacteria_in_eye': [
    '609-8',    # Bacteria identified in Eye by Aerobe culture
  ],
  'bacteria_in_nose': [
    '10353-1',  # Bacteria identified in Nose by Aerobe culture
  ],
  'bacteria_in_specimen': [
    '6463-4',   # Bacteria identified in Specimen by Culture
    '634-6',    # Bacteria identified in Specimen by Aerobe culture
  ],
  'bacteria_in_sputum': [
    '623-9',    # Bacteria identified in Sputum by Cystic fibrosis respiratory culture
  ],
  'bacteria_in_urine': [
    '630-4',    # Bacteria identified in Urine by Culture
  ],
  'bands': [
    '764-1',    # Band form neutrophils/100 leukocytes in Blood by Manual count
    '26508-2',  # Band form neutrophils/100 leukocytes in Blood
    '26510-8',  # Band form neutrophils/100 leukocytes in Body fluid
    '35332-6'   # Band form neutrophils/100 leukocytes in Blood by Automated count
  ],
  'basophils_abs': [
    '704-7',   # Basophils [#/volume] in Blood by Automated count
    '26444-0', # Basophils [#/volume] in Blood
    '705-4'    # Basophils [#/volume] in Blood by Manual count
  ],
  'basophils_percent': [
    '706-2',   # Basophils/100 leukocytes in Blood by Automated count
    '30180-4', # Basophils/100 leukocytes in Blood
    '707-0',   # Basophils/100 leukocytes in Blood by Manual count
    '28543-7'  # Basophils/100 leukocytes in Body fluid
  ],
  'bicarbonate': [
    '1959-6',   # Bicarbonate [Moles/volume] in Blood
    '1963-8',   # Bicarbonate [Moles/volume] in Serum or Plasma
    '1962-0',   # Bicarbonate [Moles/volume] in Plasma
  ],
  'bicarbonate_arterial': [
    '1960-4',  # Bicarbonate [Moles/volume] in Arterial blood
    '28640-1'  # Bicarbonate [Moles/volume] in Arterial cord blood
  ],
  'bicarbonate_capillary': [
    '1961-2'   # Bicarbonate [Moles/volume] in Capillary blood
  ],
  'bicarbonate_mixed_venous': [
    '19229-4'  # Bicarbonate [Moles/volume] in Mixed venous blood
  ],
  'bicarbonate_venous': [
    '28641-9', # Bicarbonate [Moles/volume] in Venous cord blood
    '14627-4'  # Bicarbonate [Moles/volume] in Venous blood
  ],
  'bile_acid': [
    '14628-2',   # Bile acid [Moles/volume] in Serum or Plasma
  ],
  'bilirubin': [
    '1975-2',   # Bilirubin.total [Mass/volume] in Serum or Plasma
    '1968-7',   # Bilirubin.direct [Mass/volume] in Serum or Plasma
    '1971-1',   # Bilirubin.indirect [Mass/volume] in Serum or Plasma
    '1974-5',   # Bilirubin.total [Mass/volume] in Body fluid
    '42719-5',  # Bilirubin.total [Mass/volume] in Blood
  ],
#   'bilirubin_conjugated': [
#     '15152-2',   # Bilirubin.conjugated [Mass/volume] in Serum or Plasma
#     '33898-8',   # Bilirubin.conjugated+indirect [Mass/volume] in Serum or Plasma
#   ],
#   'bilirubin_cord': [
#     '48623-3',   # Bilirubin.direct [Mass/volume] in Cord blood
#     '48624-1',   # Bilirubin.total [Mass/volume] in Cord blood
#   ],
#   'bilirubin_neonatal': [
#     '50189-0',  # Neonatal bilirubin panel [Mass/volume] - Serum or Plasma
#   ],
  'bilirubin_presence_urine': [
    '58450-8',   # Bilirubin [Presence] in Urine by Confirmatory method
  ],
  'bilirubin_total_presence_urine': [
    '5770-3',   # Bilirubin.total [Presence] in Urine by Test strip
    '50551-1',  # Bilirubin.total [Presence] in Urine by Automated test strip
  ],
  'bilirubin_urine': [
    '53327-3',  # Bilirubin.total [Mass/volume] in Urine by Automated test strip
  ],
  'blasts_abs': [
    '30376-8',   # Blasts [#/volume] in Blood
  ],
  'blasts_percent': [
    '26446-5',   # Blasts/100 leukocytes in Blood
  ],
  'bnp': [
    '33762-6',  # Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma
    '30934-4'   # Natriuretic peptide B [Mass/volume] in Serum or Plasma
  ],
  'bun': [
    '3094-0',   # Urea nitrogen [Mass/volume] in Serum or Plasma
    '14937-7',  # Urea nitrogen [Moles/volume] in Serum or Plasma
    '6299-2',   # Urea nitrogen [Mass/volume] in Blood
    '12962-7'   # Urea nitrogen [Mass/volume] in Venous blood
  ],
  'bun_creatinine': [
    '3097-3',   # Urea nitrogen/Creatinine [Mass Ratio] in Serum or Plasma
    '44734-2'   # Urea nitrogen/Creatinine [Mass Ratio] in Blood
  ],
  'calcium': [
    '17861-6',   # Calcium [Mass/volume] in Serum or Plasma
  ],
#   'calcium_ionized': [
#     '57333-7',   # Calcium.ionized [Mass/volume] adjusted to pH 7.4 in Serum or Plasma by Ion-selective membrane electrode (ISE)
#   ],
  'cancer_ag_125': [
    '10334-1',   # Cancer Ag 125 [Units/volume] in Serum or Plasma
  ],
  'cd3_cells_percent': [
    '8124-0',   # CD3 cells/100 cells in Blood
  ],
  'chloride': [
    '2075-0',   # Chloride [Moles/volume] in Serum or Plasma
    '2069-3',   # Chloride [Moles/volume] in Blood
    '51590-8',  # Chloride [Moles/volume] in Capillary blood
    '41650-3'   # Chloride [Moles/volume] in Arterial blood
  ],
  'cholesterol': [
    '2093-3',   # Cholesterol [Mass/volume] in Serum or Plasma
  ],
  'cholesterol_hdl': [
    '2085-9',    # Cholesterol in HDL [Mass/volume] in Serum or Plasma
  ],
  'cholesterol_hdl_ratio': [
    '9830-1',   # Cholesterol.total/Cholesterol in HDL [Mass Ratio] in Serum or Plasma
  ],
  'cholesterol_ldl': [
    '13457-7',   # Cholesterol in LDL [Mass/volume] in Serum or Plasma by calculation
  ],
#   'co2_total': [
#     '2028-9'    # Carbon dioxide, total [Moles/volume] in Serum or Plasma
#   ],
  'color_of_urine': [
    '5778-6',   # Color of Urine
    '50553-7',  # Color of Urine by Auto
  ],
  'covid_19_ab_presence': [
    '94563-4',   # SARS-CoV-2 (COVID-19) IgG Ab [Presence] in Serum or Plasma by Immunoassay
    '94507-1',   # SARS-CoV-2 (COVID-19) IgG Ab [Presence] in Serum, Plasma or Blood by Rapid immunoassay
    '94558-4',   # SARS-CoV-2 (COVID-19) Ag [Presence] in Respiratory specimen by Rapid immunoassay
    '94547-7',   # SARS-CoV-2 (COVID-19) IgG+IgM Ab [Presence] in Serum or Plasma by Immunoassay
    '94762-2',   # SARS-CoV-2 (COVID-19) Ab [Presence] in Serum or Plasma by Immunoassay
    '94564-2',   # SARS-CoV-2 (COVID-19) IgM Ab [Presence] in Serum or Plasma by Immunoassay
  ],
  'covid_19_naa_presence': [
    '94500-6',   # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection
    '94565-9',   # SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with non-probe detection
    '88627-5',   # Human coronavirus RNA [Presence] in Upper respiratory specimen by NAA with probe detection
    '94309-2',   # SARS-CoV-2 (COVID-19) RNA [Presence] in Specimen by NAA with probe detection
    '94306-8',   # SARS-CoV-2 (COVID-19) RNA panel - Specimen by NAA with probe detection
    '94759-8',   # SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with probe detection
    '94531-1',   # SARS-CoV-2 (COVID-19) RNA panel - Respiratory specimen by NAA with probe detection
    '94559-2',   # SARS-CoV-2 (COVID-19) ORF1ab region [Presence] in Respiratory specimen by NAA with probe detection
    '94533-7',   # SARS-CoV-2 (COVID-19) N gene [Presence] in Respiratory specimen by NAA with probe detection
  ],
  'creatinine': [
    '2160-0',   # Creatinine [Mass/volume] in Serum or Plasma
    '38483-4'   # Creatinine [Mass/volume] in Blood
  ],
#   'creatinine_urine': [
#     '2161-8',   # Creatinine [Mass/volume] in Urine
#   ],
  'crp': [
    '1988-5'   # C reactive protein [Mass/volume] in Serum or Plasma
  ],
  'd_dimer': [
    '48065-7',  # Fibrin D-dimer FEU [Mass/volume] in Platelet poor plasma
    '48067-3',  # Fibrin D-dimer FEU [Mass/volume] in Platelet poor plasma by Immunoassay
    '48058-2',  # Fibrin D-dimer DDU [Mass/volume] in Platelet poor plasma by Immunoassay
    '30240-6',  # Deprecated Fibrin D-dimer
    '48066-5'   # Fibrin D-dimer DDU [Mass/volume] in Platelet poor plasma
  ],
  'egfr_not_aa': [
    '48642-3',  # Glomerular filtration rate/1.73 sq M.predicted among non-blacks [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD)
    '33914-3',  # Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum or Plasma by Creatinine-based formula (MDRD)
    '62238-1',  # Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (CKD-EPI)
    '88294-4',  # Glomerular filtration rate/1.73 sq M.predicted among non-blacks [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (CKD-EPI)
    '77147-7'   # Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD)
  ],
  'egfr_aa': [
    '48643-1',  # Glomerular filtration rate/1.73 sq M.predicted among blacks [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD)
    '88293-6'   # Glomerular filtration rate/1.73 sq M.predicted among blacks [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (CKD-EPI)
  ],
  'eosinophils_abs': [
    '711-2',    # Eosinophils [#/volume] in Blood by Automated count
    '26449-9',  # Eosinophils [#/volume] in Blood
    '712-0'     # Eosinophils [#/volume] in Blood by Manual count
  ],
  'eosinophils_percent': [
    '713-8',    # Eosinophils/100 leukocytes in Blood by Automated count
    '26450-7',  # Eosinophils/100 leukocytes in Blood
    '714-6',    # Eosinophils/100 leukocytes in Blood by Manual count
    '26452-3',  # Eosinophils/100 leukocytes in Body fluid
    '12209-3'   # Eosinophils/100 leukocytes in Body fluid by Manual count
  ],
  'erythrocytes_urine': [
    '20409-9',  # Erythrocytes [#/volume] in Urine by Test strip
  ],
  'f2_gene_mutations': [
    '24476-4',   # F2 gene mutations found [Identifier] in Blood or Tissue by Molecular genetics method Nominal
  ],
  'ferritin': [
    '2276-4',   # Ferritin [Mass/volume] in Serum or Plasma
    '24373-3'   # Ferritin [Mass/volume] in Blood
  ],
  'fibrinogen': [
    '3255-7',   # Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay
    '55452-7'   # Deprecated Fibrinogen in Platelet poor plasma
  ],
  'follitropin': [
    '15067-2',   # Follitropin [Units/volume] in Serum or Plasma
  ],
  'globulin': [
    '10834-0',  # Globulin [Mass/volume] in Serum by calculation
    '2336-6'    # Globulin [Mass/volume] in Serum
  ],
  'glucose': [
    '2339-0',   # Glucose [Mass/volume] in Blood
    '2345-7',   # Glucose [Mass/volume] in Serum or Plasma
    '2340-8',   # Glucose [Mass/volume] in Blood by Automated test strip
  ],
#   'glucose_capillary': [
#     '41653-7',   # Glucose [Mass/volume] in Capillary blood by Glucometer
#   ],
#   'glucose_capillary_conc': [
#     '14743-9',   # Glucose [Moles/volume] in Capillary blood by Glucometer
#   ],
#   'glucose_urine': [
#     '5792-7',   # Glucose [Mass/volume] in Urine by Test strip
#     '53328-1',  # Glucose [Mass/volume] in Urine by Automated test strip
#   ],
  'glucose_presence_urine': [
    '25428-4',  # Glucose [Presence] in Urine by Test strip
  ],
  'granulocytes_abs': [
    '20482-6',   # Granulocytes [#/volume] in Blood by Automated count
  ],
  'granulocytes_percent': [
    '19023-1',   # Granulocytes/100 leukocytes in Blood by Automated count
  ],
  'hcg_blood': [
    '21198-7',  # Choriogonadotropin.beta subunit [Units/volume] in Serum or Plasma
    '19080-1',  # Choriogonadotropin [Units/volume] in Serum or Plasma
    '45194-8',  # Choriogonadotropin.intact+Beta subunit [Units/volume] in Serum or Plasma
    '30243-0',  # Choriogonadotropin.intact [Units/volume] in Serum or Plasma
    '19180-9',  # Choriogonadotropin.beta subunit free [Units/volume] in Serum or Plasma
    '53959-3'   # Choriogonadotropin.tumor marker [Units/volume] in Serum or Plasma
  ],
  'hcg_presence_blood': [
    '2118-8',  # Choriogonadotropin (pregnancy test) [Presence] in Serum or Plasma
    '2110-5'   # Choriogonadotropin.beta subunit (pregnancy test) [Presence] in Serum or Plasma
  ],
  'hcg_urine': [
    '2114-7'   # Choriogonadotropin.beta subunit [Moles/volume] in Urine
  ],
  'hcg_presence_urine': [
    '2106-3',  # Choriogonadotropin (pregnancy test) [Presence] in Urine
    '2112-1'   # Choriogonadotropin.beta subunit (pregnancy test) [Presence] in Urine
  ],
  'hematocrit': [
    '4544-3',   # Hematocrit [Volume Fraction] of Blood by Automated count
    '20570-8',  # Hematocrit [Volume Fraction] of Blood
    '32354-3'   # Hematocrit [Volume Fraction] of Arterial blood
  ],
  'hemoglobin': [
    '718-7',    # Hemoglobin [Mass/volume] in Blood
    '55782-7',  # Hemoglobin [Mass/volume] in Blood by Oximetry
  ],
  'hemoglobin_venous': [
    '30350-3',   # Hemoglobin [Mass/volume] in Venous blood
  ],
  'immature_granulocytes_abs': [
    '53115-2',  # Immature granulocytes [#/volume] in Blood by Automated count
    '51584-1'   # Immature granulocytes [#/volume] in Blood
  ],
  'immature_granulocytes_percent': [
    '71695-1',  # Immature granulocytes/100 leukocytes in Blood by Automated count
    '38518-7'   # Immature granulocytes/100 leukocytes in Blood
  ],
  'immature_granulocytes_presence': [
    '34165-1'   # Immature granulocytes [Presence] in Blood by Automated count
  ],
  'inr': [
    '6301-6',   # INR in Platelet poor plasma by Coagulation assay
    '34714-6',  # INR in Blood by Coagulation assay
    '46418-0',  # INR in Capillary blood by Coagulation assay
    '38875-1'   # INR in Platelet poor plasma or blood by Coagulation assay
  ],
#   'ketones_urine': [
#     '5797-6',   # Ketones [Mass/volume] in Urine by Test strip
#     '50557-8',  # Ketones [Mass/volume] in Urine by Automated test strip
#   ],
  'ketones_presence_urine': [
    '2514-8',   # Ketones [Presence] in Urine by Test strip
  ],
  'lactate_dehydrogenase': [
    '2532-0',   # Lactate dehydrogenase [Enzymatic activity/volume] in Serum or Plasma
    '14804-9',  # Lactate dehydrogenase [Enzymatic activity/volume] in Serum or Plasma by Lactate to pyruvate reaction
    '14803-1',  # Lactate dehydrogenase [Enzymatic activity/volume] in Body fluid by Lactate to pyruvate reaction
    '2529-6'    # Lactate dehydrogenase [Enzymatic activity/volume] in Body fluid
  ],
  'leukocyte_esterase_presence_urine': [
    '5799-2',   # Leukocyte esterase [Presence] in Urine by Test strip
    '60026-2',  # Leukocyte esterase [Presence] in Urine by Automated test strip
  ],
  'lymphocytes_abs': [
    '731-0',     # Lymphocytes [#/volume] in Blood by Automated count
    '26474-7',   # Lymphocytes [#/volume] in Blood
    '30364-4',   # Lymphocytes [#/volume] in Blood by Flow cytometry (FC)
    '732-8',     # Lymphocytes [#/volume] in Blood by Manual count
  ],
#   'lymphocytes_bronchial': [
#     '14819-7',   # Lymphocytes/100 leukocytes in Bronchial specimen by Manual count
#   ],
  'lymphocytes_percent': [
    '736-9',    # Lymphocytes/100 leukocytes in Blood by Automated count
    '737-7',    # Lymphocytes/100 leukocytes in Blood by Manual count
    '30365-1',  # Lymphocytes/100 leukocytes in Blood by Flow cytometry (FC)
    '26478-8',  # Lymphocytes/100 leukocytes in Blood
  ],
  'lymphocytes_variant_abs': [
    '26477-0',   # Variant lymphocytes [#/volume] in Blood
  ],
  'lymphocytes_variant_percent': [
    '13046-8',   # Variant lymphocytes/100 leukocytes in Blood
  ],
  'magnesium': [
    '19123-9',  # Magnesium [Mass/volume] in Serum or Plasma
  ],
  'mch': [
    '785-6',   # MCH [Entitic mass] by Automated count
  ],
  'mchc': [
    '786-4',   # MCHC [Mass/volume] by Automated count
  ],
  'mcv': [
    '787-2',   # MCV [Entitic volume] by Automated count
  ],
  'metamyelocytes_abs': [
    '30433-7',   # Metamyelocytes [#/volume] in Blood
  ],
  'metamyelocytes_percent': [
    '28541-1',   # Metamyelocytes/100 leukocytes in Blood
  ],
  'monoblasts_percent': [
    '34923-3',   # Monoblasts/100 leukocytes in Blood
  ],
  'monocytes_abs': [
    '742-7',    # Monocytes [#/volume] in Blood by Automated count
    '26484-6',  # Monocytes [#/volume] in Blood
    '743-5'     # Monocytes [#/volume] in Blood by Manual count
  ],
#   'monocytes_percent': [
#     '5905-5',   # Monocytes/100 leukocytes in Blood by Automated count
#     '26485-3',  # Monocytes/100 leukocytes in Blood
#     '744-3'     # Monocytes/100 leukocytes in Blood by Manual count
#   ],
#   'mononuclear_cells_bronchial_percent': [
#     '32807-0',   # Mononuclear cells/100 leukocytes in Bronchial specimen
#   ],
  'mycobacterium_in_specimen': [
    '543-9',    # Mycobacterium sp identified in Specimen by Organism specific culture
  ],
  'myeloblasts_abs': [
    '30444-4',   # Myeloblasts [#/volume] in Blood
  ],
  'myelocytes_abs': [
    '30446-9',   # Myelocytes [#/volume] in Blood
  ],
  'myelocytes_percent': [
    '26498-6',   # Myelocytes/100 leukocytes in Blood
  ],
  'neutrophils_abs': [
    '751-8',     # Neutrophils [#/volume] in Blood by Automated count
    '26499-4',   # Neutrophils [#/volume] in Blood
    '753-4',     # Neutrophils [#/volume] in Blood by Manual count
  ],
#   'neutrophils_bronchial_percent': [
#     '32804-7',  # Neutrophils/100 leukocytes in Bronchial specimen by Manual count
#   ],
  'neutrophils_percent': [
    '770-8',     # Neutrophils/100 leukocytes in Blood by Automated count
    '26511-6',   # Neutrophils/100 leukocytes in Blood
    '23761-0',   # Neutrophils/100 leukocytes in Blood by Manual count
    '71676-1',   # Neutrophils/Leukocytes [Pure number fraction] in Blood by Automated count
  ],
  'neutrophils_segmented_abs': [
    '30451-9'   # Segmented neutrophils [#/volume] in Blood
  ],
#   'neutrophils_segmented_percent': [
#     '769-0',    # Segmented neutrophils/100 leukocytes in Blood by Manual count
#     '32200-8'   # Segmented neutrophils/100 leukocytes in Blood by Automated count
#   ],
  'nitrite_presence_urine': [
    '5802-4',   # Nitrite [Presence] in Urine by Test strip
    '50558-6',  # Nitrite [Presence] in Urine by Automated test strip
  ],
  'pco2_arterial': [
    '2019-8',   # Carbon dioxide [Partial pressure] in Arterial blood
    '28644-3',  # Carbon dioxide [Partial pressure] in Arterial cord blood
    '32771-8',  # Carbon dioxide [Partial pressure] adjusted to patient's actual temperature in Arterial blood
  ],
  'pco2_capillary': [
    '2020-6',   # Carbon dioxide [Partial pressure] in Capillary blood
    '33022-5',  # Carbon dioxide [Partial pressure] in Capillary blood by Transcutaneous CO2 monitor
  ],
  'pco2_venous': [
    '2021-4',   # Carbon dioxide [Partial pressure] in Venous blood
    '28645-0',  # Carbon dioxide [Partial pressure] in Venous cord blood
  ],
#   'pdw_volume': [
#     '32207-3',   # Platelet distribution width [Entitic volume] in Blood by Automated count
#   ],
  'ph_blood': [
    '11558-4',  # pH of Blood
    '2753-2',   # pH of Serum or Plasma
  ],
#   'ph_arterial': [
#     '2744-1',   # pH of Arterial blood
#     '33254-4',  # pH of Arterial blood adjusted to patient's actual temperature
#   ],
  'ph_urine': [
    '50560-2',  # pH of Urine by Automated test strip
    '2756-5',   # pH of Urine
    '27378-9',  # pH of 24 hour Urine
    '5803-2',   # pH of Urine by Test strip
  ],
  'ph_venous': [
    '19213-8'   # pH of Mixed venous blood
  ],
  'phosphate': [
    '2777-1',   # Phosphate [Mass/volume] in Serum or Plasma
  ],
  'platelet': [
    '777-3',   # Platelets [#/volume] in Blood by Automated count
    '26515-7'  # Platelets [#/volume] in Blood
  ],
  'platelet_mean_volume': [
    '32623-1',   # Platelet mean volume [Entitic volume] in Blood by Automated count
  ],
  'potassium': [
    '2823-3',   # Potassium [Moles/volume] in Serum or Plasma
    '6298-4',   # Potassium [Moles/volume] in Blood
    '32713-0',  # Potassium [Moles/volume] in Arterial blood
    '39790-1'   # Potassium [Moles/volume] in Capillary blood
  ],
  'prolactin': [
    '15081-3',   # Prolactin [Units/volume] in Serum or Plasma
  ],
  'protein_blood': [
    '2885-2'   # Protein [Mass/volume] in Serum or Plasma
  ],
#   'protein_urine': [
#     '2888-6',   # Protein [Mass/volume] in Urine
#     '5804-0',   # Protein [Mass/volume] in Urine by Test strip
#     '50561-0'   # Protein [Mass/volume] in Urine by Automated test strip
#   ],
  'protein_presence_urine': [
    '20454-5'   # Protein [Presence] in Urine by Test strip
  ],
  'prothrombin_time': [
    '5902-2',   # Prothrombin time (PT)
    '5964-2',   # Prothrombin time (PT) in Blood by Coagulation assay
    '46417-2'   # Prothrombin time (PT) in Capillary blood by Coagulation assay
  ],
  'ptt': [
    '14979-9',  # aPTT in Platelet poor plasma by Coagulation assay
    '3173-2',   # aPTT in Blood by Coagulation assay
    '13488-2',  # aPTT in Control Platelet poor plasma by Coagulation assay
    '5946-9',   # aPTT.factor substitution in Platelet poor plasma by Coagulation assay --immediately after addition of normal plasma
    '43734-3'   # aPTT in Platelet poor plasma by Coagulation 1:1 saline
  ],
  'rbc': [
    '789-8',   # Erythrocytes [#/volume] in Blood by Automated count
    '26453-1'  # Erythrocytes [#/volume] in Blood
  ],
  'rdw': [
    '788-0',   # Erythrocyte distribution width [Ratio] by Automated count
  ],
  'rdw_volume': [
    '21000-5',   # Erythrocyte distribution width [Entitic volume] by Automated count
  ],
  'sodium': [
    '2951-2',   # Sodium [Moles/volume] in Serum or Plasma
    '2947-0',   # Sodium [Moles/volume] in Blood
    '42570-2',  # Sodium [Mass or Moles/volume] in Serum or Plasma
    '39792-7',  # Sodium [Moles/volume] in Capillary blood
    '32717-1'   # Sodium [Moles/volume] in Arterial blood
  ],
  'specific_gravity_urine': [
    '5811-5',   # Specific gravity of Urine by Test strip
    '2965-2',   # Specific gravity of Urine
    '50562-8',  # Specific gravity of Urine by Refractometry automated
    '5810-7',   # Specific gravity of Urine by Refractometry
    '53326-5',  # Specific gravity of Urine by Automated test strip
  ],
  'thrombin_time': [
    '3243-3',   # Thrombin time
    '46717-5',  # Thrombin time.factor substitution in Platelet poor plasma by Coagulation assay --immediately after addition of bovine thrombin
    '5955-0'    # Thrombin time in Control Platelet poor plasma by Coagulation assay
  ],
  'thyrotropin': [
    '11580-8',   # Thyrotropin [Units/volume] in Serum or Plasma by Detection limit <= 0.005 mIU/L
    '3016-3',    # Thyrotropin [Units/volume] in Serum or Plasma
    '11579-0',   # Thyrotropin [Units/volume] in Serum or Plasma by Detection limit <= 0.05 mIU/L
  ],
  'thyroxine': [
    '3026-2',   # Thyroxine (T4) [Mass/volume] in Serum or Plasma
  ],
  'thyroxine_free': [
    '3024-7',   # Thyroxine (T4) free [Mass/volume] in Serum or Plasma
  ],
  'triglyceride': [
    '2571-8',   # Triglyceride [Mass/volume] in Serum or Plasma
    '3043-7',   # Triglyceride [Mass/volume] in Blood
  ],
  'troponin': [
    '10839-9',  # Troponin I.cardiac [Mass/volume] in Serum or Plasma
    '49563-0',  # Troponin I.cardiac [Mass/volume] in Serum or Plasma by Detection limit <= 0.01 ng/mL
    '42757-5',  # Troponin I.cardiac [Mass/volume] in Blood
    '89579-7',  # Troponin I.cardiac [Mass/volume] in Serum or Plasma by High sensitivity method
  ],
  'urobilinogen_urine': [
    '20405-7',  # Urobilinogen [Mass/volume] in Urine by Test strip
  ],
  'wbc_blood': [
    '6690-2',   # Leukocytes [#/volume] in Blood by Automated count
    '26464-8',  # Leukocytes [#/volume] in Blood
    '33256-9',  # Leukocytes [#/volume] corrected for nucleated erythrocytes in Blood by Automated count
  ],
  'wbc_urine': [
    '5821-4'   # Leukocytes [#/area] in Urine sediment by Microscopy high power field
  ]
}

# COMMAND ----------

# DBTITLE 1,Track excluded LOINC codes
cc_lab_loinc_exclusion_list = [
  '19146-0',    # Reference lab test results
  '24323-8',    # Comprehensive metabolic 2000 panel - Serum or Plasma
  '8251-1',     # Service comment
  '57022-6',    # CBC W Reflex Manual Differential panel - Blood
  '57023-4',    # Auto Differential panel - Blood
  '24362-6',    # Renal function 2000 panel - Serum or Plasma
  '24321-2',    # Basic metabolic 2000 panel - Serum or Plasma
  '31208-2',    # Specimen source identified
  '33511-7',    # Appearance of Specimen
  '33512-5',    # Color of Specimen
  '18482-0',    # Yeast [Presence] in Specimen by Organism specific culture
  '77202-0',    # Laboratory comment [Text] in Report Narrative
  '50008-2',    # Mixing studies [Interpretation] in Platelet poor plasma Narrative
  '14869-2',    # Pathologist review of Blood tests
  '59462-2',    # Clinical biochemist review of results
  '44083-4',    # Extractable nuclear Ab [Interpretation] in Serum
  '25700-6',    # Immunofixation for Serum or Plasma
  '49586-1',    # First and Second trimester integrated maternal screen [Interpretation]
  '13440-3',    # Immunofixation for Urine
  '49293-4',    # Oligoclonal bands [Interpretation] in Cerebral spinal fluid by Electrophoresis Narrative
  '27045-4',    # Microscopic exam [Interpretation] of Urine by Cytology
  '49092-0',    # Second trimester quad maternal screen [Interpretation] in Serum or Plasma Narrative
  '49246-2',    # Alpha-1-Fetoprotein interpretation in Serum or Plasma Narrative
  '41273-4',    # Alpha-1-Fetoprotein interpretation in Amniotic fluid
  '13169-8',    # Immunoelectrophoresis for Serum or Plasma
  '76479-5',    # Acetylcholinesterase [Interpretation] in Amniotic fluid Narrative
  '24325-3',    # Hepatic function 2000 panel - Serum or Plasma
  '48767-8',    # Annotation comment [Interpretation] Narrative
  '6742-1',     # Erythrocyte morphology finding [Identifier] in Blood
]

# COMMAND ----------

# DBTITLE 1,Define lab feature definitions for Translator
translator_lab_feature_definitions = {
  'adenovirus_positive_from_lab': {
    'id': 'NCIT:C141278',
    'name': 'Adenovirus Positive',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['adenovirus_pcr_naat_pos']
  },
  'clostridium_difficile_positive_from_lab': {
    'id': 'NCIT:C146650',
    'name': 'Clostridium difficile Positive',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['clostridium_difficile_pcr_naat_pos']
  },
  'herpes_simplex_virus_infection_from_lab': {
    'id': 'NCIT:C155871',
    'name': 'Herpes Simplex Virus Infection',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['herpes_simplex_virus_pcr_naat_pos']
  },
  'influenza_a_virus_positive_from_lab': {
    'id': 'NCIT:C167118',
    'name': 'Influenza A Virus Positive',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['influenza_a_pcr_naat_pos']
  },
  'influenza_b_virus_positive_from_lab': {
    'id': 'NCIT:C167118',
    'name': 'Influenza B Virus Positive',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['influenza_b_pcr_naat_pos']
  },
  'sars_coronavirus_2_positive_from_lab': {
    'id': 'NCIT:C171647',
    'name': 'SARS Coronavirus 2 Positive',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['sars_cov_2_pcr_naat_pos']
  },
  'sars_coronavirus_2_negative_from_lab': {
    'id': 'NONE',
    'name': 'SARS Coronavirus 2 Negative',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['sars_cov_2_pcr_naat_neg']
  },
  'hyperalbuminemia_from_lab': {
    'id': 'HP:0012117',
    'name': 'Hyperalbuminemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['albumin_high']
  },
  'hypoalbuminemia_from_lab': {
    'id': 'HP:0003073',
    'name': 'Hypoalbuminemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['albumin_low']
  },
  'elevated_alkaline_phosphatase_from_lab': {
    'id': 'HP:0003155',
    'name': 'Elevated alkaline phosphatase',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['alp_high']
  },
  'low_alkaline_phosphatase_from_lab': {
    'id': 'HP:0003282',
    'name': 'Low alkaline phosphatase',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['alp_low']
  },
  'elevated_serum_alanine_aminotransferase_from_lab': {
    'id': 'HP:0031964',
    'name': 'Elevated serum alanine aminotransferase',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['alt_high']
  },
  'elevated_serum_anion_gap_from_lab': {
    'id': 'HP:0031962',
    'name': 'Elevated serum anion gap',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['anion_gap_high']
  },
  'decreased_serum_anion_gap_from_lab': {
    'id': 'HP:0031963',
    'name': 'Decreased serum anion gap',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['anion_gap_low']
  },
  'abnormal_macroscopic_urine_appearance_from_lab': {
    'id': 'HP:0033072',
    'name': 'Abnormal macroscopic urine appearance',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['appearance_of_urine_abnormal']
  },
  'elevated_serum_aspartate_aminotransferase_from_lab': {
    'id': 'HP:0031956',
    'name': 'Elevated serum aspartate aminotransferase',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ast_high']
  },
  'increased_circulating_band_cell_count_from_lab': {
    'id': 'HP:0032239',
    'name': 'Increased circulating band cell count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bands_high']
  },
  'increased_basophil_count_from_lab': {
    'id': 'HP:0031807',
    'name': 'Increased basophil count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['basophils_abs_high', 'basophils_percent_high']
  },
  'decreased_basophil_count_from_lab': {
    'id': 'HP:0031808',
    'name': 'Decreased basophil count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['basophils_abs_low', 'basophils_percent_low']
  },
  'elevated_serum_bicarbonate_concentration_from_lab': {
    'id': 'HP:0032067',
    'name': 'Elevated serum bicarbonate concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bicarbonate_high']
  },
  'decreased_serum_bicarbonate_concentration_from_lab': {
    'id': 'HP:0032066',
    'name': 'Decreased serum bicarbonate concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bicarbonate_low']
  },
  'increased_serum_bile_acid_concentration_from_lab': {
    'id': 'HP:0012202',
    'name': 'Increased serum bile acid concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bile_acid_high']
  },
  'decreased_serum_bile_concentration_from_lab': {
    'id': 'HP:0030985',
    'name': 'Decreased serum bile concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bile_acid_low']
  },
  'hyperbilirubinemia_from_lab': {
    'id': 'HP:0002904',
    'name': 'Hyperbilirubinemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bilirubin_high']
  },
  'increased_peripheral_blast_count_from_lab': {
    'id': 'HP:0032372',
    'name': 'Increased peripheral blast count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['blasts_abs_high', 'blasts_percent_high']
  },
  'increased_circulating_brain_natriuretic_peptide_concentration_from_lab': {
    'id': 'HP:0033534',
    'name': 'Increased circulating brain natriuretic peptide concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bnp_high']
  },
  'increased_blood_urea_nitrogen_from_lab': {
    'id': 'HP:0003138',
    'name': 'Increased blood urea nitrogen',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bun_high']
  },
  'reduced_blood_urea_nitrogen_from_lab': {
    'id': 'HP:0031969',
    'name': 'Reduced blood urea nitrogen',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['bun_low']
  },
  'hypercalcemia_from_lab': {
    'id': 'HP:0003072',
    'name': 'Hypercalcemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['calcium_high']
  },
  'hypocalcemia_from_lab': {
    'id': 'HP:0002901',
    'name': 'Hypocalcemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['calcium_low']
  },
  'elevated_carcinoma_antigen_125_level_from_lab': {
    'id': 'HP:0031030',
    'name': 'Elevated carcinoma antigen 125 level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cancer_ag_125_high']
  },
  'decreased_proportion_of_cd3_positive_t_cells_from_lab': {
    'id': 'HP:0045080',
    'name': 'Decreased proportion of CD3-positive T cells',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cd3_cells_percent_low']
  },
  'hyperchloremia_from_lab': {
    'id': 'HP:0011423',
    'name': 'Hyperchloremia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['chloride_high']
  },
  'hypochloremia_from_lab': {
    'id': 'HP:0003113',
    'name': 'Hypochloremia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['chloride_low']
  },
  'hypercholesterolemia_from_lab': {
    'id': 'HP:0003124',
    'name': 'Hypercholesterolemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_high']
  },
  'hypocholesterolemia_from_lab': {
    'id': 'HP:0003146',
    'name': 'Hypocholesterolemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_low']
  },
  'increased_hdl_cholesterol_concentration_from_lab': {
    'id': 'HP:0012184',
    'name': 'Increased HDL cholesterol concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_hdl_high']
  },
  'decreased_hdl_cholesterol_concentration_from_lab': {
    'id': 'HP:0003233',
    'name': 'Decreased HDL cholesterol concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_hdl_low']
  },
  'increased_ldl_cholesterol_concentration_from_lab': {
    'id': 'HP:0003141',
    'name': 'Increased LDL cholesterol concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_ldl_high']
  },
  'decreased_ldl_cholesterol_concentration_from_lab': {
    'id': 'HP:0003563',
    'name': 'Decreased LDL cholesterol concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['cholesterol_ldl_low']
  },
  'abnormal_urinary_color_from_lab': {
    'id': 'HP:0012086',
    'name': 'Abnormal urinary color',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['color_of_urine_abnormal']
  },
  'elevated_serum_creatinine_from_lab': {
    'id': 'HP:0003259',
    'name': 'Elevated serum creatinine',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['creatinine_high']
  },
  'decreased_serum_creatinine_from_lab': {
    'id': 'HP:0012101',
    'name': 'Decreased serum creatinine',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['creatinine_low']
  },
  'elevated_c_reactive_protein_level_from_lab': {
    'id': 'HP:0011227',
    'name': 'Elevated C-reactive protein level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['crp_high']
  },
  'elevated_d_dimers_from_lab': {
    'id': 'HP:0033106',
    'name': 'Elevated D-dimers',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['d_dimer_high']
  },
  'increased_glomerular_filtration_rate_from_lab': {
    'id': 'HP:0012214',
    'name': 'Increased glomerular filtration rate',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['egfr_not_aa_high', 'egfr_aa_high']
  },
  'decreased_glomerular_filtration_rate_from_lab': {
    'id': 'HP:0012213',
    'name': 'Decreased glomerular filtration rate',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['egfr_not_aa_low', 'egfr_aa_low']
  },
  'eosinophilia_from_lab': {
    'id': 'HP:0001880',
    'name': 'Eosinophilia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['eosinophils_abs_high', 'eosinophils_percent_high']
  },
  'decreased_eosinophil_count_from_lab': {
    'id': 'HP:0031891',
    'name': 'Decreased eosinophil count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['eosinophils_abs_low', 'eosinophils_percent_low']
  },
  'hematuria_from_lab': {
    'id': 'HP:0000790',
    'name': 'Hematuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['erythrocytes_urine_high']
  },
  'increased_circulating_ferritin_concentration_from_lab': {
    'id': 'HP:0003281',
    'name': 'Increased circulating ferritin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ferritin_high']
  },
  'decreased_circulating_ferritin_concentration_from_lab': {
    'id': 'HP:0012343',
    'name': 'Decreased circulating ferritin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ferritin_low']
  },
  'hyperfibrinogenemia_from_lab': {
    'id': 'HP:0011899',
    'name': 'Hyperfibrinogenemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['fibrinogen_high']
  },
  'hypofibrinogenemia_from_lab': {
    'id': 'HP:0011900',
    'name': 'Hypofibrinogenemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['fibrinogen_low']
  },
  'increased_circulating_globulin_level_from_lab': {
    'id': 'HP:0032311',
    'name': 'Increased circulating globulin level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['globulin_high']
  },
  'decreased_circulating_globulin_level_from_lab': {
    'id': 'HP:0032312',
    'name': 'Decreased circulating globulin level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['globulin_low']
  },
  'hyperglycemia_from_lab': {
    'id': 'HP:0003074',
    'name': 'Hyperglycemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['glucose_high']
  },
  'hypoglycemia_from_lab': {
    'id': 'HP:0001943',
    'name': 'Hypoglycemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['glucose_low']
  },
  'abnormality_of_urine_glucose_concentration_from_lab': {
    'id': 'HP:0011016',
    'name': 'Abnormality of urine glucose concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['glucose_presence_urine_abnormal']
  },
  'abnormal_granulocyte_count_from_lab': {
    'id': 'HP:0032309',
    'name': 'Abnormal granulocyte count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['granulocytes_abs_high', 'granulocytes_abs_low', 'granulocytes_percent_high', 'granulocytes_percent_low']
  },
  'increased_hematocrit_from_lab': {
    'id': 'HP:0001899',
    'name': 'Increased hematocrit',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['hematocrit_high']
  },
  'reduced_hematocrit_from_lab': {
    'id': 'HP:0031851',
    'name': 'Reduced hematocrit',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['hematocrit_low']
  },
  'increased_hemoglobin_from_lab': {
    'id': 'HP:0001900',
    'name': 'Increased hemoglobin',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['hemoglobin_high']
  },
  'decreased_hemoglobin_concentration_from_lab': {
    'id': 'HP:0020062',
    'name': 'Decreased hemoglobin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['hemoglobin_low']
  },
  'increased_circulating_immature_neutrophil_count_from_lab': {
    'id': 'HP:0032236',
    'name': 'Increased circulating immature neutrophil count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['immature_granulocytes_abs_high', 'immature_granulocytes_percent_high']
  },
  'prolonged_prothrombin_time_from_lab': {
    'id': 'HP:0008151',
    'name': 'Prolonged prothrombin time',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['inr_high', 'prothrombin_time_high']
  },
  'decreased_prothrombin_time_from_lab': {
    'id': 'HP:0032198',
    'name': 'Decreased prothrombin time',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['inr_low', 'prothrombin_time_low']
  },
  'ketonuria_from_lab': {
    'id': 'HP:0002919',
    'name': 'Ketonuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ketones_presence_urine_abnormal']
  },
  'increased_lactate_dehydrogenase_level_from_lab': {
    'id': 'HP:0025435',
    'name': 'Increased lactate dehydrogenase level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['lactate_dehydrogenase_high']
  },
  'pyuria_from_lab': {
    'id': 'HP:0012085',
    'name': 'Pyuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['leukocyte_esterase_presence_urine_abnormal', 'wbc_urine_high']
  },
  'abnormal_lymphocyte_count_from_lab': {
    'id': 'HP:0040088',
    'name': 'Abnormal lymphocyte count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['lymphocytes_abs_high', 'lymphocytes_abs_low', 'lymphocytes_percent_high', 'lymphocytes_percent_low']
  },
  'hypermagnesemia_from_lab': {
    'id': 'HP:0002918',
    'name': 'Hypermagnesemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['magnesium_high']
  },
  'hypomagnesemia_from_lab': {
    'id': 'HP:0002917',
    'name': 'Hypomagnesemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['magnesium_low']
  },
  'increased_mean_corpuscular_hemoglobin_concentration_from_lab': {
    'id': 'HP:0025548',
    'name': 'Increased mean corpuscular hemoglobin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['mchc_high']
  },
  'decreased_mean_corpuscular_hemoglobin_concentration_from_lab': {
    'id': 'HP:0025547',
    'name': 'Decreased mean corpuscular hemoglobin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['mchc_low']
  },
  'increased_mean_corpuscular_volume_from_lab': {
    'id': 'HP:0005518',
    'name': 'Increased mean corpuscular volume',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['mcv_high']
  },
  'decreased_mean_corpuscular_volume_from_lab': {
    'id': 'HP:0025066',
    'name': 'Decreased mean corpuscular volume',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['mcv_low']
  },
  'increased_circulating_metamyelocyte_count_from_lab': {
    'id': 'HP:0032238',
    'name': 'Increased circulating metamyelocyte count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['metamyelocytes_abs_high', 'metamyelocytes_percent_high']
  },
  'increased_circulating_myelocyte_count_from_lab': {
    'id': 'HP:0032237',
    'name': 'Increased circulating myelocyte count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['myelocytes_abs_high', 'myelocytes_percent_high']
  },
  'abnormal_neutrophil_count_from_lab': {
    'id': 'HP:0011991',
    'name': 'Abnormal neutrophil count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['neutrophils_abs_high', 'neutrophils_abs_low', 'neutrophils_percent_high', 'neutrophils_percent_low']
  },
  'hypercapnia_from_lab': {
    'id': 'HP:0012416',
    'name': 'Hypercapnia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['pco2_arterial_high']
  },
  'hypocapnia_from_lab': {
    'id': 'HP:0012417',
    'name': 'Hypocapnia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['pco2_arterial_low']
  },
  'abnormal_urine_ph_from_lab': {
    'id': 'HP:0032943',
    'name': 'Abnormal urine pH',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ph_urine_high', 'ph_urine_low']
  },
  'hyperphosphatemia_from_lab': {
    'id': 'HP:0002905',
    'name': 'Hyperphosphatemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['phosphate_high']
  },
  'hypophosphatemia_from_lab': {
    'id': 'HP:0002148',
    'name': 'Hypophosphatemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['phosphate_low']
  },
  'thrombocytosis_from_lab': {
    'id': 'HP:0001894',
    'name': 'Thrombocytosis',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['platelet_high']
  },
  'thrombocytopenia_from_lab': {
    'id': 'HP:0001873',
    'name': 'Thrombocytopenia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['platelet_low']
  },
  'increased_mean_platelet_volume_from_lab': {
    'id': 'HP:0011877',
    'name': 'Increased mean platelet volume',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['platelet_mean_volume_high']
  },
  'decreased_mean_platelet_volume_from_lab': {
    'id': 'HP:0005537',
    'name': 'Decreased mean platelet volume',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['platelet_mean_volume_low']
  },
  'hyperkalemia_from_lab': {
    'id': 'HP:0002153',
    'name': 'Hyperkalemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['potassium_high']
  },
  'hypokalemia_from_lab': {
    'id': 'HP:0002900',
    'name': 'Hypokalemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['potassium_low']
  },
  'increased_circulating_prolactin_concentration_from_lab': {
    'id': 'HP:0000870',
    'name': 'Increased circulating prolactin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['prolactin_high']
  },
  'reduced_circulating_prolactin_concentration_from_lab': {
    'id': 'HP:0008202',
    'name': 'Reduced circulating prolactin concentration',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['prolactin_low']
  },
  'proteinuria_from_lab': {
    'id': 'HP:0000093',
    'name': 'Proteinuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['protein_presence_urine_abnormal']
  },
  'prolonged_partial_thromboplastin_time_from_lab': {
    'id': 'HP:0003645',
    'name': 'Prolonged partial thromboplastin time',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['ptt_high']
  },
  'increased_red_blood_cell_count_from_lab': {
    'id': 'HP:0020059',
    'name': 'Increased red blood cell count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['rbc_high']
  },
  'decreased_red_blood_cell_count_from_lab': {
    'id': 'HP:0020060',
    'name': 'Decreased red blood cell count',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['rbc_low']
  },
  'increased_rbc_distribution_width_from_lab': {
    'id': 'HP:0031965',
    'name': 'Increased RBC distribution width',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['rdw_high', 'rdw_volume_high']
  },
  'hypernatremia_from_lab': {
    'id': 'HP:0003228',
    'name': 'Hypernatremia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['sodium_high']
  },
  'hyponatremia_from_lab': {
    'id': 'HP:0002902',
    'name': 'Hyponatremia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['sodium_low']
  },
  'hyperosthenuria_from_lab': {
    'id': 'HP:0033359',
    'name': 'Hyperosthenuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['specific_gravity_urine_high']
  },
  'hyposthenuria_from_lab': {
    'id': 'HP:0003158',
    'name': 'Hyposthenuria',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['specific_gravity_urine_low']
  },
  'increased_thyroid_stimulating_hormone_level_from_lab': {
    'id': 'HP:0002925',
    'name': 'Increased thyroid-stimulating hormone level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyrotropin_high']
  },
  'decreased_thyroid_stimulating_hormone_level_from_lab': {
    'id': 'HP:0031098',
    'name': 'Decreased thyroid-stimulating hormone level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyrotropin_low']
  },
  'increased_circulating_t4_level_from_lab': {
    'id': 'HP:0031506',
    'name': 'Increased circulating T4 level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyroxine_high']
  },
  'decreased_circulating_t4_level_from_lab': {
    'id': 'HP:0031507',
    'name': 'Decreased circulating T4 level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyroxine_low']
  },
  'increased_circulating_free_t4_level_from_lab': {
    'id': 'HP:0033077',
    'name': 'Increased circulating free T4 level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyroxine_free_high']
  },
  'decreased_circulating_free_t4_level_from_lab': {
    'id': 'HP:0033078',
    'name': 'Decreased circulating free T4 level',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['thyroxine_free_low']
  },
  'hypertriglyceridemia_from_lab': {
    'id': 'HP:0002155',
    'name': 'Hypertriglyceridemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['triglyceride_high']
  },
  'hypotriglyceridemia_from_lab': {
    'id': 'HP:0012153',
    'name': 'Hypotriglyceridemia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['triglyceride_low']
  },
  'increased_troponin_i_level_in_blood_from_lab': {
    'id': 'HP:0410173',
    'name': 'Increased troponin I level in blood',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['troponin_high']
  },
  'increased_urine_urobilinogen_from_lab': {
    'id': 'HP:0031890',
    'name': 'Increased urine urobilinogen',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['urobilinogen_urine_high']
  },
  'decreased_urine_urobilinogen_from_lab': {
    'id': 'HP:0032473',
    'name': 'Decreased urine urobilinogen',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['urobilinogen_urine_low']
  },
  'leukocytosis_from_lab': {
    'id': 'HP:0001974',
    'name': 'Leukocytosis',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['wbc_blood_high']
  },
  'leukopenia_from_lab': {
    'id': 'HP:0001882',
    'name': 'Leukopenia',
    'category': 'biolink:PhenotypicFeature',
    'cc_list': ['wbc_blood_low']
  },
}

# COMMAND ----------

# DBTITLE 1,Sanity check the dictionary, please make sure that those two key list are the same length, and the difference list contains no key
## Sanity check the dictionary, please make sure that those two key list are the same length, and the difference list contains no key
list1 = list(cc_lab_loinc_dictionary.keys())
print(len(list1))

list2 = list(translator_lab_feature_definitions.keys())
print(len(list2))

print(set(list2) - set(list1))
print(len(list(set(list2) - set(list1))))