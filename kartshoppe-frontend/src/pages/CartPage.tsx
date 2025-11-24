import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useCart } from "../contexts/CartContext";
import { EventTracker } from "../services/EventTracker";
import { useProductCache } from "../contexts/ProductCacheContext";

const CartPage: React.FC = () => {
  const navigate = useNavigate();
  const { items, totalAmount, removeFromCart, updateQuantity, clearCart } =
    useCart();
  const { getProduct } = useProductCache();

  React.useEffect(() => {
    EventTracker.trackPageView("/cart");
  }, []);

  const handleCheckout = () => {
    EventTracker.trackEvent("CHECKOUT_START", {
      cartTotal: totalAmount,
      itemCount: items.length,
    });
    navigate("/checkout");
  };

  if (items.length === 0) {
    return (
      <div className="max-w-4xl mx-auto">
        <div className="card p-12 text-center">
          <div className="text-6xl mb-4">🛒</div>
          <h2 className="text-2xl font-bold text-gray-800 mb-4">
            Your cart is empty
          </h2>
          <p className="text-gray-600 mb-6">
            Add some products to get started!
          </p>
          <Link to="/products" className="btn-primary">
            Continue Shopping
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-6xl mx-auto">
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Shopping Cart</h1>

      <div className="grid grid-cols-3 gap-8">
        <div className="col-span-2 space-y-4">
          {items.map((item) => {
            const product = getProduct(item.productId);
            const availableStock = product?.inventory ?? 0;

            return (
              <div key={item.productId} className="card p-6 animate-fade-in">
                <div className="flex space-x-4">
                  <div className="w-32 h-32 rounded-lg overflow-hidden bg-gray-200">
                    <img
                      src={item.imageUrl}
                      alt={item.productName}
                      className="w-full h-full object-cover"
                    />
                  </div>

                  <div className="flex-1">
                    <h3 className="text-lg font-semibold text-gray-800 mb-2">
                      {item.productName}
                    </h3>
                    <p className="text-gray-600 mb-4">
                      ${item.price.toFixed(2)} each
                    </p>

                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <button
                          onClick={() =>
                            updateQuantity(item.productId, item.quantity - 1)
                          }
                          className="w-8 h-8 rounded bg-gray-200 hover:bg-gray-300 flex items-center justify-center"
                        >
                          -
                        </button>
                        <span className="w-12 text-center font-medium">
                          {item.quantity}
                        </span>
                        <button
                          onClick={() => {
                            if (item.quantity < availableStock) {
                              updateQuantity(item.productId, item.quantity + 1);
                            }
                          }}
                          disabled={item.quantity >= availableStock}
                          className="w-8 h-8 rounded bg-gray-200 hover:bg-gray-300 flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          +
                        </button>
                      </div>

                      <div className="flex items-center space-x-4">
                        <span className="text-lg font-bold text-gray-900">
                          ${(item.price * item.quantity).toFixed(2)}
                        </span>
                        <button
                          onClick={() => removeFromCart(item.productId)}
                          className="text-red-500 hover:text-red-700"
                        >
                          <svg
                            className="w-5 h-5"
                            fill="none"
                            stroke="currentColor"
                            viewBox="0 0 24 24"
                          >
                            <path
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              strokeWidth={2}
                              d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
                            />
                          </svg>
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}

          <button
            onClick={clearCart}
            className="text-red-600 hover:text-red-700 font-medium"
          >
            Clear Cart
          </button>
        </div>

        {/* Order Summary */}
        <div>
          <div className="card p-6 sticky top-24">
            <h2 className="text-xl font-bold text-gray-900 mb-4">
              Order Summary
            </h2>

            <div className="space-y-2 mb-4">
              <div className="flex justify-between text-gray-600">
                <span>
                  Subtotal (
                  {items.reduce((sum, item) => sum + item.quantity, 0)} items)
                </span>
                <span>${totalAmount.toFixed(2)}</span>
              </div>
              <div className="flex justify-between text-gray-600">
                <span>Estimated Tax</span>
                <span>${(totalAmount * 0.08).toFixed(2)}</span>
              </div>
              <div className="flex justify-between text-gray-600">
                <span>Shipping</span>
                <span>{totalAmount > 50 ? "FREE" : "$5.99"}</span>
              </div>
            </div>

            <div className="border-t pt-4 mb-6">
              <div className="flex justify-between text-xl font-bold text-gray-900">
                <span>Total</span>
                <span>
                  $
                  {(
                    totalAmount +
                    totalAmount * 0.08 +
                    (totalAmount > 50 ? 0 : 5.99)
                  ).toFixed(2)}
                </span>
              </div>
            </div>

            <button
              onClick={handleCheckout}
              className="btn-primary w-full mb-3"
            >
              Proceed to Checkout
            </button>

            <Link
              to="/products"
              className="block text-center text-primary-600 hover:text-primary-700 font-medium"
            >
              Continue Shopping
            </Link>

            <div className="mt-6 p-4 bg-green-50 rounded-lg">
              <p className="text-sm text-green-800">
                ✓ Secure checkout powered by event sourcing
              </p>
              <p className="text-sm text-green-800 mt-1">
                ✓ Real-time inventory updates
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CartPage;
