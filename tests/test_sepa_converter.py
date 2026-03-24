import pathlib
import sys
from datetime import datetime

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from qlik_sense_mcp_server.sepa_converter import Invoice, LineItem, Party, build_sepa_xml


def test_build_sepa_xml_contains_metadata():
    debtor = Party(name="Westdeutscher Metall-Handel S.L.", iban="ES8200817041100002741976", bic="BSABESBBXXX", country_code="ES")
    creditor = Party(name="Herzog & Partner slu", iban="ES8200817041100002741976", bic="BSABESBBXXX", country_code="ES")
    invoice = Invoice(
        invoice_number="066-26",
        invoice_date=datetime(2026, 3, 23),
        debtor=debtor,
        creditor=creditor,
        amount=6338.71,
        currency="EUR",
        line_items=[LineItem(description="Schwimmbadpumpe", quantity=1, unit_price=529.0, total=529.0, vat_percent=21)],
        reference="Sabadell Upload",
    )

    xml = build_sepa_xml(invoice, msg_id="066-26-TEST")

    assert "MsgId>066-26-TEST" in xml
    assert "<Nm>Westdeutscher Metall-Handel S.L.</Nm>" in xml
    assert "<IBAN>ES8200817041100002741976</IBAN>" in xml
    assert "<Ustrd>Sabadell Upload</Ustrd>" in xml
    assert "<Cd>SEPA</Cd>" in xml
