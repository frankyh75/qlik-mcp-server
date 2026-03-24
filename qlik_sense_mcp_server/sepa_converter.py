from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List


@dataclass
class LineItem:
    description: str
    quantity: float
    unit_price: float
    total: float
    vat_percent: float


@dataclass
class Party:
    name: str
    iban: str
    bic: str
    country_code: str


@dataclass
class Invoice:
    invoice_number: str
    invoice_date: datetime
    debtor: Party
    creditor: Party
    amount: float
    currency: str
    line_items: List[LineItem]
    reference: str


def _iso_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def build_sepa_xml(invoice: Invoice, msg_id: str | None = None) -> str:
    msg_id = msg_id or f"{invoice.invoice_number}-{int(datetime.utcnow().timestamp())}"
    creation_date = datetime.utcnow().isoformat()

    lines = []
    for idx, item in enumerate(invoice.line_items, start=1):
        lines.append(
            """
            <PmtId>
                <InstrId>{instr}</InstrId>
                <EndToEndId>{e2e}</EndToEndId>
            </PmtId>
            <Amt>
                <InstdAmt Ccy="{ccy}">{amt:.2f}</InstdAmt>
            </Amt>
            <CdtrAgt>
                <FinInstnId>
                    <BIC>{bic}</BIC>
                </FinInstnId>
            </CdtrAgt>
            <Cdtr>
                <Nm>{name}</Nm>
            </Cdtr>
            <CdtrAcct>
                <Id>
                    <IBAN>{iban}</IBAN>
                </Id>
            </CdtrAcct>
        """.format(
                instr=f"{invoice.invoice_number}-{idx}",
                e2e=invoice.invoice_number,
                ccy=invoice.currency,
                amt=invoice.amount,
                bic=invoice.creditor.bic,
                name=invoice.creditor.name,
                iban=invoice.creditor.iban,
            )
        )

    detail = "\n".join(lines)
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pain.001.001.03\">
  <CstmrCdtTrfInitn>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>{creation_date}</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <CtrlSum>{invoice.amount:.2f}</CtrlSum>
      <InitgPty>
        <Nm>{invoice.debtor.name}</Nm>
      </InitgPty>
    </GrpHdr>
    <PmtInf>
      <PmtInfId>{msg_id}</PmtInfId>
      <PmtMtd>TRF</PmtMtd>
      <BtchBookg>false</BtchBookg>
      <NbOfTxs>1</NbOfTxs>
      <CtrlSum>{invoice.amount:.2f}</CtrlSum>
      <PmtTpInf>
        <SvcLvl>
          <Cd>SEPA</Cd>
        </SvcLvl>
      </PmtTpInf>
      <ReqdExctnDt>{_iso_date(invoice.invoice_date)}</ReqdExctnDt>
      <Dbtr>
        <Nm>{invoice.debtor.name}</Nm>
      </Dbtr>
      <DbtrAcct>
        <Id>
          <IBAN>{invoice.debtor.iban}</IBAN>
        </Id>
      </DbtrAcct>
      <DbtrAgt>
        <FinInstnId>
          <BIC>{invoice.debtor.bic}</BIC>
        </FinInstnId>
      </DbtrAgt>
      {detail}
      <RmtInf>
        <Ustrd>{invoice.reference}</Ustrd>
      </RmtInf>
    </PmtInf>
  </CstmrCdtTrfInitn>
</Document>
"""
