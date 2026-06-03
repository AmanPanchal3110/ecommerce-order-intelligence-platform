from faker import Faker
import random
import uuid
from datetime import datetime, timezone, timedelta 

fake=Faker()


def generate_order_data():
    items=[]
    order_status=[]
    total_amount=0
    
    for i in range(random.randint(1,3)):
        price=random.randint(100,10000)
        quantity=random.randint(1,5)
        total_amount+=price*quantity

        items.append({
            "product_id": fake.bothify(text="p####"),
            "quantity": quantity,
            "price": price
        })
    event_time = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1,8))
    ingestion_time = datetime.now(timezone.utc)
    create_time = event_time
    payyment_type=random.choice(["PREPAID","COD"])
    is_cancelled=random.random()<0.1
    if payyment_type=="COD":
        if is_cancelled:
            order_status.append({"order_status":"CREATED","payment_status":"PENDING","status_timestamp": create_time.isoformat()})
            order_status.append({"order_status":"CANCELLED","payment_status":"FAILED","status_timestamp": (create_time +timedelta(minutes=random.randint(2, 5))).isoformat()})
        else:
            order_status.append({"order_status":"CREATED","payment_status":"PENDING","status_timestamp": create_time.isoformat()})
            order_status.append({"order_status":"SHIPPED","payment_status":"PENDING","status_timestamp": (create_time +timedelta(hours=random.randint(5, 10))).isoformat()})
            order_status.append({"order_status":"DELIVERED","payment_status":"SUCCESS","status_timestamp": (create_time +timedelta(days=random.randint(1, 2))).isoformat()})
    else:
        if is_cancelled:
            order_status.append({"order_status":"CREATED","payment_status":"PENDING","status_timestamp": create_time.isoformat()})
            order_status.append({"order_status":"CANCELLED","payment_status":"FAILED","status_timestamp": (create_time +timedelta(minutes=random.randint(2, 5))).isoformat()})
        else:
            order_status.append({"order_status":"CREATED","payment_status":"PENDING","status_timestamp": create_time.isoformat()})
            order_status.append({"order_status":"CONFIRMED","payment_status":"SUCCESS","status_timestamp": (create_time +timedelta(minutes=random.randint(2,5))).isoformat()})
            order_status.append({"order_status":"SHIPPED","payment_status":"SUCCESS","status_timestamp": (create_time +timedelta(hours=random.randint(5, 10))).isoformat()})
            order_status.append({"order_status":"DELIVERED","payment_status":"SUCCESS","status_timestamp": (create_time +timedelta(days=random.randint(1, 2))).isoformat()})

    return {
            "event_id":str(uuid.uuid4()),
            "event_type":"order",
            "event_timestamp":event_time.isoformat(),
            "order_id":"ord-"+str(uuid.uuid4()),
            "customer_id":fake.bothify(text="cus###"),
            "customer_name":fake.name(),
            "customer_email":fake.email(),
            "items":items,
            "total_amount":total_amount,
            "payment_type":payyment_type,
            "currency":"INR",
            "order_status":order_status,
            "ingestion_timestamp": ingestion_time.isoformat() 
    }


def generate_payment_data(order):
    if order["payment_type"] == "COD":
        method = "COD"
        provider = card_network = issuing_bank = None
    else:
        method=random.choice(["UPI","CARD","NETBANKING"])
        if method=="UPI":
            provider = random.choice(["GPay", "PhonePe", "Paytm"])
            card_network = None
            issuing_bank = None
        elif method=="CARD":
            provider = None
            card_network = random.choice(["VISA", "MASTERCARD", "RUPAY"])
            issuing_bank = random.choice(["HDFC", "ICICI", "SBI"])
        else:
            provider = random.choice(["HDFC", "ICICI", "SBI"])
            card_network = None
            issuing_bank = None

    return {
        "event_id":str(uuid.uuid4()),
        "event_type":"payment", 
        "event_timestamp":order["event_timestamp"],
        "order_id":order["order_id"],
        "customer_id":order["customer_id"],
        "payment_id": "pay-" + str(uuid.uuid4()),
        "payment_method":method,
        "payment_provider":provider,
        "card_network":card_network,
        "issuing_bank":issuing_bank,
        "amount":order["total_amount"],
        "currency":"INR",
        "payment_type":order["payment_type"],
        "ingestion_timestamp": order["ingestion_timestamp"]
    }

def generate_return_data(order):
    return{
        "event_id":str(uuid.uuid4()),
        "event_type":"return",
        "event_timestamp":order["event_timestamp"],
        "order_id":order["order_id"],
        "customer_id":order["customer_id"],
        "return_id": "ret-" + str(uuid.uuid4()),
        "return_reason":random.choice(["DAMAGED","WRONG_ITEM","NO_LONGER_NEEDED"]),
        "return_status":random.choice(["INITIATED","APPROVED","REJECTED"]),
        "refund_amount":order["total_amount"],
        "currency":"INR",
        "ingestion_timestamp": order["ingestion_timestamp"]
    }
def generate_order_corrupt_data(order,corruption_rate=0.05):
    if random.random() > corruption_rate:
        return order
    else:
        record=order.copy()
        corruption_type=random.choice([
            "negative_price",
            "zero_quantity",
            "future_event_timestamp"
        ])
        if corruption_type=="negative_price":
            record["items"][0]["price"]=-abs(record["items"][0]["price"])
        elif corruption_type=="zero_quantity":
            record["items"][0]["quantity"]=0
        elif corruption_type=="future_event_timestamp":
            record["event_timestamp"] = (
            datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))
            ).isoformat()
        return record
    
def generate_payment_corrupt_data(payment,corruption_rate=0.08):
    if random.random() > corruption_rate:
        return payment
    else:
        record=payment.copy()
        corruption_type=random.choice([
            "amount_mismatch",
            "null_payment_method",
            "future_event_timestamp"
        ])
        if corruption_type=="amount_mismatch":
            record["amount"]=record["amount"]+random.randint(1,100)
        elif corruption_type=="null_payment_method":
            record["payment_method"]=None
        elif corruption_type=="future_event_timestamp":
            record["event_timestamp"] = (
            datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))
            ).isoformat()
        return record