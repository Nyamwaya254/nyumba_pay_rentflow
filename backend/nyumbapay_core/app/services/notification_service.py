"""Notification service _ Africa's Talking SMS + WhatsApp
Message templates are defined here. All sends are logged to
notification_logs regardless of success or failure.
"""

import asyncio
from decimal import Decimal
from typing import Any, Callable, Protocol, cast
import africastalking
import httpx
import structlog

from nyumbapay_core.app.core.config import Settings
from nyumbapay_core.app.models.models import NotificationType


logger = structlog.get_logger(__name__)


class _SMSClient(Protocol):
    """Structural type describing the Africa's Talking SMS service object"""

    def send(
        self,
        message: str,
        recipients: list[str],
        sender_id: str | None = None,
        enqueue: bool = False,
        callback: Callable[..., Any] | None = None,
        timeout: tuple[float, float] = (3.05, 9.05),
    ) -> dict[str, Any]: ...


def _fmt(amount: Decimal) -> str:
    """Format Decimal as KES string e.g KES 15,000.00"""
    return f"KES {amount:,.2f}"


class NotificationService:
    """Sends SMS and WhatsAPP via Africa's Talking"""

    def __init__(self, settings: Settings) -> None:
        africastalking.initialize(settings.at_username, settings.at_api_key)
        self._sms: _SMSClient = cast(_SMSClient, africastalking.SMS)
        self._settings = settings
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=5.0,  # time to establish TCP connection to AT servers
                read=10.0,  # time to receive first response byte
                write=5.0,  # time to finish sending the request body
                pool=5.0,  # time to acquire a connection from the pool
            ),
            limits=httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=30.0,
            ),
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool"""

        await self._http_client.aclose()

    # public send methods
    async def send_rent_reminder(
        self,
        phone: str,
        tenant_name: str,
        period: str,
        base_rent: Decimal,
        water_charge: Decimal,
        garbage_charge: Decimal,
        total_due: Decimal,
        paybill_number: str,
        account_reference: str,
        business_name: str,
        reminder_type: NotificationType,
        previous_balance: Decimal = Decimal("0"),
    ) -> tuple[bool, str]:
        """Send the appropriate rent reminder message via SMS and WhatsApp"""
        if reminder_type == NotificationType.REMINDER_28:
            body = self._template_28(
                tenant_name,
                period,
                base_rent,
                water_charge,
                garbage_charge,
                total_due,
                paybill_number,
                account_reference,
                business_name,
                previous_balance,
            )
        elif reminder_type == NotificationType.REMINDER_1ST:
            body = self._template_1st(
                tenant_name,
                period,
                total_due,
                paybill_number,
                account_reference,
                business_name,
            )
        else:
            body = self._template_5th(
                tenant_name,
                period,
                total_due,
                paybill_number,
                account_reference,
                business_name,
            )

        # Dispatch both channels concurrently
        (sms_ok, sms_id), (wa_ok, wa_id) = await asyncio.gather(
            self._send_sms(phone, body), self._send_whatsapp(phone, body)
        )

        success = sms_ok or wa_ok
        msg_id = sms_id or wa_id or ""
        return success, msg_id

    async def send_payment_confirmation(
        self,
        phone: str,
        tenant_name: str,
        amount: Decimal,
        receipt_number: str,
        balance: Decimal,
        transaction_date: str,
        business_name: str,
    ) -> tuple[bool, str]:
        """Send payment received confirmation via SMS and WhatsApp"""
        body = self._template_confirmation_paid(
            tenant_name,
            amount,
            receipt_number,
            balance,
            transaction_date,
            business_name,
        )
        (sms_ok, sms_id), (wa_ok, wa_id) = await asyncio.gather(
            self._send_sms(phone, body),
            self._send_whatsapp(phone, body),
        )
        return sms_ok or wa_ok, sms_id or wa_id or ""

    async def send_welcome(
        self,
        phone: str,
        tenant_name: str,
        paybill_number: str,
        account_reference: str,
        rent_amount: Decimal,
        business_name: str,
    ) -> tuple[bool, str]:
        """Send welcome message with M-pesa payment details on lease activation"""
        body = self._template_welcome(
            tenant_name,
            business_name,
            rent_amount,
            paybill_number,
            account_reference,
        )
        (sms_ok, sms_id), (wa_ok, wa_id) = await asyncio.gather(
            self._send_sms(phone, body), self._send_whatsapp(phone, body)
        )
        return sms_ok or wa_ok, sms_id or wa_id or ""

    # message templates

    @staticmethod
    def _template_28(
        name: str,
        period: str,
        base_rent: Decimal,
        water: Decimal,
        garbage: Decimal,
        total: Decimal,
        paybill: str,
        acc: str,
        biz: str,
        prev_balance: Decimal,
    ) -> str:
        lines = [
            f"Dear {name}, your rent for {period} is due.",
            f"Rent: {_fmt(base_rent)}",
            f"Water: {_fmt(water)}",
            f"Garbage: {_fmt(garbage)}",
        ]
        if prev_balance > 0:
            lines.append(f"Previous Arrears: {_fmt(prev_balance)}")
        elif prev_balance < 0:
            credit_amount = abs(prev_balance)
            lines.append(
                f"Credit Balance(overpayment): {_fmt(credit_amount)}"
                f"Deducted from this month total rent"
            )
        lines += [
            f"Total: {_fmt(total)}",
            f"Pay via M-Pesa Paybill: {paybill}",
            f"Account: {acc}",
            f"— {biz}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _template_1st(
        name: str,
        period: str,
        total: Decimal,
        paybill: str,
        acc: str,
        biz: str,
    ) -> str:
        """Send sms to tenant about last month outstanding balance only"""
        return (
            f"Dear {name}, your rent of {_fmt(total)} for {period} "
            f"is still outstanding. Please pay immediately kindly.\n"
            f"Paybill: {paybill} Account: {acc}\n— {biz}"
        )

    @staticmethod
    def _template_5th(
        name: str,
        period: str,
        total: Decimal,
        paybill: str,
        acc: str,
        biz: str,
    ) -> str:
        """Rent reminder for %th of the month"""
        return (
            f"URGENT: Dear {name}, {_fmt(total)} rent arrears for {period} "
            f"remain unpaid. Failure to pay may result in further action.\n"
            f"Paybill: {paybill} Account: {acc}\n— {biz}"
        )

    @staticmethod
    def _template_confirmation_paid(
        name: str,
        amount: Decimal,
        receipt: str,
        balance: Decimal,
        txn_date: str,
        biz: str,
    ) -> str:
        """Template for when a tenant pays their rent"""

        if balance > 0:
            balance_line = f"Outstanding: {_fmt(balance)}"
        elif balance < 0:
            balance_line = (
                f"Credit Balance (overpayment): {_fmt(abs(balance))}. Thank you!"
            )
        else:
            balance_line = "Account fully paid. Thank you!"

        return (
            f"Dear {name}, payment of {_fmt(amount)} received on {txn_date}.\n"
            f"M-Pesa Ref: {receipt}\n"
            f"{balance_line}\n"
            f"— {biz}"
        )

    @staticmethod
    def _template_welcome(
        name: str, biz: str, rent: Decimal, paybill: str, acc: str
    ) -> str:
        """Welcome message sent on lease activation with M-Pesa payment details"""
        return (
            f"Welcome {name}! Your tenancy with {biz} is active.\n"
            f"Monthly Rent: {_fmt(rent)}\n"
            f"Pay via M-Pesa Paybill: {paybill}\n"
            f"Account Number: {acc}\n"
            f"Save this information — you'll use it every month."
        )

    # transport
    async def _send_sms(self, phone: str, body: str) -> tuple[bool, str]:
        """Send SMS via Africa's talking SDk"""
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._sms.send(
                    message=body,
                    recipients=[phone],
                    sender_id=self._settings.at_sender_id,
                ),
            )
            recipients = response.get("SMSMessageData", {}).get("Recipients", [])
            if recipients and recipients[0].get("status") == "Success":
                msg_id = recipients[0].get("messageID", "")
                logger.info("sms_sent", phone=phone, msg_id=msg_id)
                return True, msg_id
            logger.warning("sms_failed", phone=phone, response=response)
            return False, ""
        except Exception as exc:
            logger.error("sms_exception", phone=phone, error=str(exc))
            return False, ""

    async def _send_whatsapp(self, phone: str, body: str) -> tuple[bool, str]:
        """Send WhatsApp message via Africa's Talking WhatsApp API
        -Uses a long-lived httpx.AsyncClient constructed in __init__ so the
         underlying TCP connection pool is reused across all calls rather than
         opened and torn down per message.
        """
        if not self._settings.at_whatsapp_number:
            return False, ""
        try:
            response = await self._http_client.post(
                self._settings.at_whatsapp_url,
                headers={
                    "apiKey": self._settings.at_api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "username": self._settings.at_username,
                    "to": phone,
                    "from": self._settings.at_whatsapp_number,
                    "message": body,
                },
            )
            data = response.json()

            if response.status_code == 200:
                msg_id = data.get("messageID", "")
                logger.info("whatsapp_sent", phone=phone, msg_id=msg_id)
                return True, msg_id
            logger.warning(
                "whatsapp_failed",
                phone=phone,
                status=response.status_code,
                response=data,
            )
            return False, ""

        except httpx.HTTPError as exc:
            logger.error("whatsapp_http_error", phone=phone, error=str(exc))
            return False, ""
