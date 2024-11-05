from typing import Dict, List

NAME_MAP: Dict[str, str] = {'_High Court Div': '', '_High Court Civil': '', '_High Court Criminal': ''}



CRIMINAL_CASES: List[str] = [
    'Murder Case',
    'Criminal Revision',
    'Criminal Appeal',
    'Murder - Gender Justice Criminal Case',
    'Criminal Court Martial Appeal',
    'Anti-Corruption and Economic Crimes Revision',
    'Criminal Miscellaneous Application',
    'Criminal Applications',
    'COA Criminal Appeal'
]

BROAD_CASE_TYPES: Dict[str, List[str]] = {
    'Civil Suit': [
        'Civil Suit',
        'Anti-Corruption and Economic Crimes Suit',
        'Family Originating Summons',
        'Family Civil Case',
        'HCC(OS) Family',
        'Commercial Admiralty',
        'Commercial Matters',
    ],
    'Adoption': ['Family Adoption'],
    'Divorce': ['Family Divorce Cause'],
    'Criminal Application': ['Criminal Miscellaneous Application'],
    'Miscellaneous Application': [
        'Civil Case Miscellaneous',
        'Judicial Review Miscellaneous',
        'JR  Petition Miscellaneous',
        'Anti-Corruption and Economic Crimes Miscellaneous',
        'Commercial Miscellaneous',
        'Constitution and Human Rights Petitions Miscellaneous',
        'Family Miscellaneous',
        'Commercial Arbitration',
    ],
    'Judicial Review': [
        'Anti-Corruption and Economic Crime Judicial review',
        'Judicial Review ELC',
        'Judicial Review',
    ],
    'Criminal Revision': [
        'Criminal Revision',
        'Anti-Corruption and Economic Crimes Revision',
    ],
    'Criminal Appeal': [
        'Criminal Appeal',
        'Criminal Court Martial Appeal',
        'Anti-Corruption and Economic Crimes Appeal',
    ],
    'Civil Appeal': [
        'Family Appeal',
        'Civil Appeal',
        'Commercial Appeal',
        'Constitution and Human Rights Election Petition Appeal',
        'Constitution and Human Rights Petition Appeal',
        'Constitution and Human Rights Election Petition Appeal',
        'Gender Justice Civil Appeal',
        'Constitution and Human Rights Miscellaneous Election Petition Appeal (MEPA)',
    ],
    'Constitution Petition': [
        'Anti Corruption and Economic Crimes Petition',
        'High Court Criminal Petition',
        'Constitution and Human Rights Petition (Civil)',
        'Constitution and Human Rights Election Petition',
        'High Court Constitution and Human Rights Petitions (Criminal)',
        'Commercial Petition',
    ],
    'Probate Administration': [
        'Family P&A Intestate',
        'Family P&A Ad Litem',
        'Family P&A Ad Colligenda',
        'Family P&A Citation',
        'Family P&A Testate',
        'Family P&A Resealing of Grant',
        'Family P&A De Bonis Non',
        'Resealing of Grant',
        'Citation-Family',
    ],
    'Murder': [
        'Murder Case',
        'Murder - Gender Justice Criminal Case',
    ],
    'Tax Appeal': [
        'Commercial Income Tax Apperiod_startpeal',
        'Commercial Custom Tax Appeal',
    ],
    'Bankruptcy and Insolvency': [
        'Commercial Insolvency Notice Petition',
        'Commercial Insolvency Petition',
        'Commercial Bankruptcy Notice',
        'Commercial Insolvency Cause',
        'Commercial Insolvency Notice',
        'Commercial Bankruptcy Cause',
        'Commercial Winding Up Cause',
    ]
}


RESOLVED_OUTCOMES: List[str] = [
    'Ruling Delivered- Case Closed',
    'Terminated',
    'Matter Settled- Case Closed',
    'Application Dismissed - Case Closed',
    'Judgment Delivered- Case Closed',
    'Matter Withdrawn',
    'Application Allowed - Case Closed',
    'Application Withdrawn - Case Closed',
    'Judgment Delivered- Convicted',
    'Placed In Probation',
    'Dismissed',
    'Judgment Delivered',
    'Judgment Delivered- Acquittal',
    'Ruling Delivered- Accused Discharged',
    'Abated',
    'Consolidated- Case Closed',
    'Grant Confirmed',
    'Limited Grant Issued',
    'Struck Out',
    'Grant Revoked',
    'Consent Recorded - Case Closed',
    'Dismissed For Want Of Prosecution - Case Closed',
    'Out Of Court Settlement Reached',
    'Appeal Dismissed',
    'Retrial',
    'Appeal Rejected',
    'Sentence Commuted',
    'Ruling Delivered- Application Closed',
    'Probation Orders Issued',
    'Order Issued - Case Closed',
    'Revision Declined'
]


TRANSFERED_CASES: List[str] = [
    'File Transfered -case Closed',
    'File Transferred',
]


MERIT_CATEGORY: Dict[str, List[str]] = {
    'Judgment Delivered': [
        'Judgment Delivered- Case Closed',
        'Judgment Delivered',
        'Judgment Delivered- Acquittal',
        'Judgment Delivered- Convicted',
        'Grant Revoked',
        'Retrial'
    ],
    'Ruling Case Closed': [
        'Ruling Delivered- Case Closed',
        'Ruling Delivered- Accused Discharged',
    ],
    'Final Grant': [
        'Grant Confirmed',
        'Limited Grant Issued',
    ],
    'Case Withdrawn': [
        'Matter Withdrawn',
        'Application Withdrawn - Case Closed',
    ],
    'Out Of Court Settlement': [
        'Consent Recorded - Case Closed',
        'Matter Settled Through Mediation',
        'Out Of Court Settlement Reached',
    ],
    'Dismissed': [
        'Dismissed For Want Of Prosecution - Case Closed',
        'Dismissed',
        'Appeal Dismissed',
        'Terminated'
    ],

    'Case Closed': [
        'Struck Out',
        'Application Dismissed - Case Closed',
        'Application Allowed - Case Closed',
        'Matter Settled- Case Closed',
        'Ruling Delivered- Application Closed',
        'Consolidated- Case Closed',
        'Abated',
        'Placed In Probation',
        'Revision Declined',
        'Probation Orders Issued',
        'Appeal Rejected',
        'Interlocutory Judgement Entered',
        'Order issued - Case closed'
    ],
}


TIME_LIMITS: Dict[str, int] = {
    'murder': 360,
    'revision': 90,
    'misc_application': 90,
    'suit': 360,
    'judicial_review': 180,
    'constitutional_petition': 180,
}


NON_ADJOURNABLE: List[str] = [
    'Taxation and Issuance of Certificates',
    'Orders',
    'Appointments of  Mediator',
    'Screening of files for Mediation',
    'Post-judgment',
    'Re-activation',
    'Reactivation',
    'Notice of Taxation',
    'Entering Interlocutory Judgments',
    'Approval by DR',
    'Registration/Filing-Application',
    'Registration/Filing',
    'Registration/Filing-Application',
]


MERIT_OUTCOMES: List[str] = [
    'Ruling Delivered- Case Closed',
    'Judgment Delivered- Case Closed',
    'Judgment Delivered',
    'Judgment Delivered- Acquittal',
    'Judgment Delivered- Convicted',
    'Grant Revoked',
    'Ruling Delivered- Accused Discharged',
    'Retrial'
]
