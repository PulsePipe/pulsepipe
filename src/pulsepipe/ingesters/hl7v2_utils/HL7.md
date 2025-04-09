# HL7 Version 2.x Message Types and Segments Reference

## Common HL7 v2.x Message Types

### Patient Administration
- ADT (Admit/Discharge/Transfer)
  - A01: Admit/Visit Notification
  - A02: Transfer a Patient
  - A03: Discharge/End Visit
  - A04: Register a Patient
  - A05: Pre-Admit a Patient
  - A06: Change an Outpatient to an Inpatient
  - A07: Change an Inpatient to an Outpatient
  - A08: Update Patient Information
  - A09: Patient Departing
  - A10: Patient Arriving
  - A11: Cancel Admit
  - A12: Cancel Transfer
  - A13: Cancel Discharge
  - A14: Pending Admit
  - A15: Pending Transfer
  - A16: Pending Discharge
  - A17: Swap Patients
  - A18: Merge Patient Information
  - A19: Patient Query
  - A20: Bed Status Update
  - A21: Patient Goes on a "Leave of Absence"
  - A22: Patient Returns from a "Leave of Absence"
  - A23: Delete a Patient Record
  - A24: Link Patient Information
  - A25: Cancel Pending Admit
  - A26: Cancel Pending Transfer
  - A27: Cancel Pending Discharge
  - A28: Add Person Information
  - A29: Delete Person Information
  - A30: Merge Person Information
  - A31: Update Person Information
  - A32: Cancel Patient Arriving
  - A33: Cancel Patient Departing
  - A34: Merge Patient Information - Patient ID Only
  - A35: Merge Patient Information - Account Number Only
  - A36: Merge Patient Information - Patient ID and Account Number

### Order Entry
- ORM (Order Message)
  - O01: Order Message
- OMI (Order Message Initiated)
- OMG (General Order Message)

### Query
- QRY: Query
- QCN: Cancel Query
- VXQ: Query for Vaccination Record
- SPQ: Stored Procedure Request

### Laboratory
- ORU (Observation Result Unsolicited)
  - R01: Unsolicited Observation Message
- OUL (Unsolicited Laboratory Observation)
- MDM (Medical Document Management)
- MFN (Master File Notification)

### Patient Scheduling
- SIU (Schedule Information Unsolicited)
  - S12: Notification of New Appointment
  - S13: Notification of Appointment Modification
  - S14: Notification of Appointment Cancellation
  - S15: Notification of Appointment Discontinuation
  - S16: Notification of Appointment Delete
  - S17: Notification of Added Ancillary Service
  - S18: Notification of Cancelled Ancillary Service
  - S19: Notification of Discontinued Ancillary Service
  - S20: Notification of Deleted Ancillary Service
  - S21: Notification of Appointment Changed to New Start Date
  - S22: Notification of Appointment Suspended
  - S23: Notification of Appointment Resumed
  - S24: Notification of Appointment Modifications
  - S25: Notification of Appointment Cancelled Due to Insufficient Resources

### Financial Management
- BAR (Billing Account Record)
- DFT (Detailed Financial Transaction)

### Medical Records
- MFN (Master File Notification)
- MFR (Master File Response)

### Other Specialized Messages
- REF (Patient Referral)
- RGV (Pharmacy/Treatment Give)
- RAS (Pharmacy/Treatment Administration)
- RDE (Pharmacy/Treatment Encoded Order)
- RER (Pharmacy/Treatment Encoded Results)
- RRE (Pharmacy/Treatment Response)
- PEX (Product Experience)
- EAR (Electronic Authorization Request)
- EHC (Encounters and Health Care Services)

## Common HL7 v2.x Segments

### Patient Identification Segments
- PID (Patient Identification)
  - PID-1: Set ID
  - PID-2: Patient ID (External)
  - PID-3: Patient Identifier List
  - PID-4: Alternate Patient ID
  - PID-5: Patient Name
  - PID-6: Mother's Maiden Name
  - PID-7: Date of Birth
  - PID-8: Sex
  - PID-9: Patient Alias
  - PID-10: Race
  - PID-11: Patient Address
  - PID-12: County Code
  - PID-13: Phone Number - Home
  - PID-14: Phone Number - Business
  - PID-15: Primary Language
  - PID-16: Marital Status
  - PID-17: Religion
  - PID-18: Patient Account Number
  - PID-19: SSN Number
  - PID-20: Driver's License Number
  - PID-21: Mother's Identifier
  - PID-22: Ethnic Group
  - PID-23: Birth Place
  - PID-24: Multiple Birth Indicator
  - PID-25: Birth Order
  - PID-26: Citizenship
  - PID-27: Veterans Military Status
  - PID-28: Nationality Code
  - PID-29: Patient Death Date and Time
  - PID-30: Patient Death Indicator

### Message Header Segment
- MSH (Message Header)
  - MSH-1: Field Separator
  - MSH-2: Encoding Characters
  - MSH-3: Sending Application
  - MSH-4: Sending Facility
  - MSH-5: Receiving Application
  - MSH-6: Receiving Facility
  - MSH-7: Date/Time of Message
  - MSH-8: Security
  - MSH-9: Message Type
  - MSH-10: Message Control ID
  - MSH-11: Processing ID
  - MSH-12: Version ID
  - MSH-13: Sequence Number
  - MSH-14: Continuation Pointer
  - MSH-15: Accept Acknowledgment Type
  - MSH-16: Application Acknowledgment Type
  - MSH-17: Country Code
  - MSH-18: Character Set
  - MSH-19: Principal Language of Message
  - MSH-20: Alternate Character Set Handling Scheme
  - MSH-21: Conformance Statement ID

### Patient Visit Segments
- PV1 (Patient Visit)
  - PV1-1: Set ID
  - PV1-2: Patient Class
  - PV1-3: Assigned Patient Location
  - PV1-4: Admission Type
  - PV1-5: Preadmit Number
  - PV1-6: Prior Patient Location
  - PV1-7: Attending Doctor
  - PV1-8: Referring Doctor
  - PV1-9: Consulting Doctor
  - PV1-10: Hospital Service
  - PV1-11: Temporary Location
  - PV1-12: Preadmit Test Indicator
  - PV1-13: Re-admission Indicator
  - PV1-14: Admit Source
  - PV1-15: Ambulatory Status
  - PV1-16: VIP Indicator
  - PV1-17: Admitting Doctor
  - PV1-18: Patient Type
  - PV1-19: Visit Number
  - PV1-20: Financial Class
  - PV1-21: Charge Price Indicator
  - PV1-22: Courtesy Code
  - PV1-23: Credit Rating
  - PV1-24: Contract Code
  - PV1-25: Contract Effective Date
  - PV1-26: Contract Amount
  - PV1-27: Contract Period
  - PV1-28: Interest Code
  - PV1-29: Transfer to Bad Debt Code
  - PV1-30: Transfer to Bad Debt Date
  - PV1-31: Bad Debt Agency Code
  - PV1-32: Bad Debt Transfer Amount
  - PV1-33: Bad Debt Recovery Amount
  - PV1-34: Delete Account Indicator
  - PV1-35: Delete Account Date
  - PV1-36: Discharge Disposition
  - PV1-37: Discharged to Location
  - PV1-38: Diet Type
  - PV1-39: Servicing Facility
  - PV1-40: Bed Status
  - PV1-41: Account Status
  - PV1-42: Pending Location
  - PV1-43: Prior Temporary Location
  - PV1-44: Admit Date/Time
  - PV1-45: Discharge Date/Time
  - PV1-46: Current Patient Balance
  - PV1-47: Total Charges
  - PV1-48: Total Adjustments
  - PV1-49: Total Payments
  - PV1-50: Alternate Visit ID
  - PV1-51: Visit Indicator
  - PV1-52: Other Healthcare Provider

### Observation Request Segments
- OBR (Observation Request)
  - OBR-1: Set ID
  - OBR-2: Placer Order Number
  - OBR-3: Filler Order Number
  - OBR-4: Universal Service ID
  - OBR-5: Priority
  - OBR-6: Requested Date/Time
  - OBR-7: Observation Date/Time
  - OBR-8: Observation End Date/Time
  - OBR-9: Collection Volume
  - OBR-10: Collector Identifier
  - OBR-11: Specimen Action Code
  - OBR-12: Danger Code
  - OBR-13: Relevant Clinical Information
  - OBR-14: Specimen Received Date/Time
  - OBR-15: Specimen Source
  - OBR-16: Ordering Provider
  - OBR-17: Order Callback Phone Number
  - OBR-18: Placer Field 1
  - OBR-19: Placer Field 2
  - OBR-20: Filler Field 1
  - OBR-21: Filler Field 2
  - OBR-22: Results Rpt/Status Chng Date/Time
  - OBR-23: Charge to Practice
  - OBR-24: Diagnostic Service Section ID
  - OBR-25: Result Status
  - OBR-26: Parent Result
  - OBR-27: Quantity/Timing
  - OBR-28: Result Copies To
  - OBR-29: Parent Result Identifier
  - OBR-30: Transportation Mode
  - OBR-31: Reason for Study
  - OBR-32: Principal Result Interpreter
  - OBR-33: Assistant Result Interpreter
  - OBR-34: Technician
  - OBR-35: Transcriptionist
  - OBR-36: Scheduled Date/Time

### Observation Result Segments
- OBX (Observation/Result)
  - OBX-1: Set ID
  - OBX-2: Value Type
  - OBX-3: Observation Identifier
  - OBX-4: Observation Sub-ID
  - OBX-5: Observation Value
  - OBX-6: Units
  - OBX-7: Reference Range
  - OBX-8: Abnormal Flags
  - OBX-9: Probability
  - OBX-10: Nature of Abnormal Test
  - OBX-11: Observation Result Status
  - OBX-12: Effective Date of Reference Range
  - OBX-13: User Defined Access Checks
  - OBX-14: Date/Time of the Observation
  - OBX-15: Producer's ID
  - OBX-16: Responsible Observer
  - OBX-17: Observation Method
  - OBX-18: Equipment Instance Identifier
  - OBX-19: Date/Time of the Analysis

### Additional Specialized Segments
- NK1: Next of Kin/Associated Parties
- IN1: Insurance
- GT1: Guarantor
- AL1: Patient Allergy Information
- DG1: Diagnosis
- PR1: Procedures
- RXO: Pharmacy/Treatment Order
- RXR: Pharmacy/Treatment Route
- RXC: Pharmacy/Treatment Component
- AIG: Appointment Information - General Resource
- AIL: Appointment Information - Location Resource
- AIP: Appointment Information - Personnel Resource

## Notes
- This list covers the most common message types and segments in HL7 v2.x
- Implementations may vary slightly between v2.3, v2.4, v2.5, and v2.6
- Always refer to the specific version's implementation guide for precise details
- Not all fields are listed for each segment to keep the reference concise

## Recommendations for Implementation
1. Use flexible parsing techniques
2. Handle optional and variable-length fields
3. Consider using robust HL7 parsing libraries
4. Validate against the specific HL7 version's standard
5. Implement error handling for unexpected message structures