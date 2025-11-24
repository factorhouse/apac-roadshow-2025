import axios from "axios";
import React, { createContext, useContext, useState, useEffect } from "react";
import { useWebSocket } from "./WebSocketContext";
import { EventTracker } from "../services/EventTracker";

interface CartItem {
  productId: string;
  productName: string;
  price: number;
  quantity: number;
  imageUrl: string;
}

interface CartContextType {
  items: CartItem[];
  totalAmount: number;
  addToCart: (product: any, quantityToAdd: number) => void;
  removeFromCart: (productId: string) => void;
  updateQuantity: (productId: string, quantity: number) => void;
  clearCart: () => void;
  itemCount: number;
}

const CartContext = createContext<CartContextType | undefined>(undefined);

export const CartProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [items, setItems] = useState<CartItem[]>([]);
  const { sendMessage } = useWebSocket();

  useEffect(() => {
    const savedCart = localStorage.getItem("cart");
    if (savedCart) {
      setItems(JSON.parse(savedCart));
    }
  }, []);

  useEffect(() => {
    localStorage.setItem("cart", JSON.stringify(items));
  }, [items]);

  const updateCartOnBackend = async (productId: string, quantity: number) => {
    try {
      const sessionId = EventTracker.getSessionId();
      await axios.post(`/api/ecommerce/cart/${sessionId}/add`, {
        productId,
        userId: EventTracker.getUserId(),
        quantity, // Send the new total quantity
      });
      console.log(
        `SUCCESS: Backend cart for product ${productId} updated to quantity ${quantity}.`
      );
      return true;
    } catch (error) {
      console.error(`!!! API CALL FAILED for product ${productId} !!!`, error);
      return false;
    }
  };

  const addToCart = async (product: any, quantityToAdd: number) => {
    const availableStock = product.inventory ?? 0;
    const existingItem = items.find(
      (item) => item.productId === product.productId
    );

    let finalQuantity;

    if (existingItem) {
      // If item exists, add the new quantity to the current quantity
      const potentialQuantity = existingItem.quantity + quantityToAdd;
      // Cap the result at the available stock to prevent overselling
      finalQuantity = Math.min(potentialQuantity, availableStock);
    } else {
      // If it's a new item, its quantity is simply the quantity to add
      // (also capped at the stock limit)
      finalQuantity = Math.min(quantityToAdd, availableStock);
    }

    // If the stock limit was hit and the quantity isn't changing, stop here.
    if (existingItem && finalQuantity <= existingItem.quantity) {
      console.warn(
        `Stock limit of ${availableStock} reached for ${product.productId}.`
      );
      return;
    }

    // Proceed with the backend update using the final, correct quantity
    const success = await updateCartOnBackend(product.productId, finalQuantity);

    if (success) {
      if (existingItem) {
        updateQuantity(product.productId, finalQuantity, true);
      } else {
        const newItem: CartItem = {
          productId: product.productId,
          productName: product.name,
          price: product.price,
          quantity: finalQuantity, // Use the correct final quantity
          imageUrl: product.imageUrl,
        };
        setItems([...items, newItem]);

        EventTracker.trackEvent("ADD_TO_CART", {
          productId: product.productId,
          value: product.price,
          quantity: finalQuantity, // Track the correct quantity
        });

        sendMessage({
          type: "CART_UPDATE",
          action: "ADD",
          payload: newItem,
        });
      }
    }
  };

  const removeFromCart = (productId: string) => {
    setItems(items.filter((item) => item.productId !== productId));

    EventTracker.trackEvent("REMOVE_FROM_CART", {
      productId,
    });

    sendMessage({
      type: "CART_UPDATE",
      action: "REMOVE",
      payload: { productId },
    });
  };

  const updateQuantity = async (
    productId: string,
    quantity: number,
    fromAddToCart = false
  ) => {
    if (quantity <= 0) {
      removeFromCart(productId);
      return;
    }

    if (!fromAddToCart) {
      const success = await updateCartOnBackend(productId, quantity);
      if (!success) return;
    }

    setItems(
      items.map((item) =>
        item.productId === productId ? { ...item, quantity } : item
      )
    );

    EventTracker.trackEvent("UPDATE_CART_QUANTITY", {
      productId,
      quantity,
    });

    sendMessage({
      type: "CART_UPDATE",
      action: "UPDATE_QUANTITY",
      payload: { productId, quantity },
    });
  };

  const clearCart = () => {
    setItems([]);
    sendMessage({
      type: "CART_UPDATE",
      action: "CLEAR",
      payload: {},
    });
  };

  const totalAmount = items.reduce(
    (sum, item) => sum + item.price * item.quantity,
    0
  );
  const itemCount = items.reduce((sum, item) => sum + item.quantity, 0);

  return (
    <CartContext.Provider
      value={{
        items,
        totalAmount,
        addToCart,
        removeFromCart,
        updateQuantity,
        clearCart,
        itemCount,
      }}
    >
      {children}
    </CartContext.Provider>
  );
};

export const useCart = () => {
  const context = useContext(CartContext);
  if (context === undefined) {
    throw new Error("useCart must be used within a CartProvider");
  }
  return context;
};
