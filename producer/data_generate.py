from faker import Faker
import random
import uuid
from datetime import datetime 

fake=Faker()


def generate_order_data():
    items=[]
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
        
    return {
            "event_id":str(uuid.uuid4()),
            "event_type":"order",
            "event_timestamp":fake.iso8601(),
            "order_id":"ord-"+str(uuid.uuid4()),
            "customer_id":fake.bothify(text="cus###"),
            "customer_name":fake.name(),
            "customer_email":fake.email(),
            "items":items,
            "total_amount":total_amount,
            "payment_type":random.choice(["PREPAID","COD"]),
            "currency":"INR",
            "order_status":"CREATED",
            "ingestion_timestamp": datetime.utcnow().isoformat()
    }


def generate_payment_data(order):
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
    
    if order["payment_type"]=="PREPAID":
        payment_status=random.choice(["SUCCESS","FAILED"])
    else:
        payment_status="PENDING"
    return {
        "event_id":str(uuid.uuid4()),
        "event_type":"payment", 
        "event_timestamp":fake.iso8601(),
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
        "payment_status":payment_status,
        "ingestion_timestamp": datetime.utcnow().isoformat()
    }

def generate_return_data(order):
    return{
        "event_id":str(uuid.uuid4()),
        "event_type":"return",
        "event_timestamp":fake.iso8601(),
        "order_id":order["order_id"],
        "customer_id":order["customer_id"],
        "return_id": "ret-" + str(uuid.uuid4()),
        "return_reason":random.choice(["DAMAGED","WRONG_ITEM","NO_LONGER_NEEDED"]),
        "return_status":random.choice(["INITIATED","APPROVED","REJECTED"]),
        "refund_amount":order["total_amount"],
        "currency":"INR",
        "ingestion_timestamp": datetime.utcnow().isoformat()
    }
    