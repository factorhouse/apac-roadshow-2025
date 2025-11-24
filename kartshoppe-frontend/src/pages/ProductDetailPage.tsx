import React, { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useCart } from "../contexts/CartContext";
import { EventTracker } from "../services/EventTracker";
import { useProductCache } from "../contexts/ProductCacheContext";

const ProductDetailPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { addToCart } = useCart();
  const { getProduct, isLoading } = useProductCache();
  const [product, setProduct] = useState<any>(null);
  const [quantity, setQuantity] = useState(1);
  const [selectedImage, setSelectedImage] = useState(0);

  useEffect(() => {
    if (id) {
      // First try to get from cache
      const cachedProduct = getProduct(id);
      if (cachedProduct) {
        setProduct(cachedProduct);
        EventTracker.trackProductView(
          cachedProduct.productId,
          cachedProduct.name,
          cachedProduct.price
        );
      } else {
        // If not in cache, fetch from API (cache will be updated via WebSocket)
        fetchProduct();
      }
    }
  }, [id, getProduct]);

  const fetchProduct = async () => {
    // Fallback if product not in cache
    // The cache will auto-update via WebSocket when product is fetched
    setProduct({
      productId: id,
      name: `Loading Product ${id}...`,
      description: `Product information is being loaded...`,
      price: 0,
      imageUrl: `https://picsum.photos/600/400?random=${id}`,
      category: "Loading",
      rating: 0,
      reviewCount: 0,
      inventory: 0,
      tags: [],
    });
  };

  const handleAddToCart = () => {
    if (product) {
      addToCart(product, quantity);
      navigate("/cart");
    }
  };

  if (isLoading) {
    return (
      <div className="max-w-6xl mx-auto">
        <div className="card p-8 animate-pulse">
          <div className="grid grid-cols-2 gap-8">
            <div className="h-96 bg-gray-200 rounded-lg" />
            <div className="space-y-4">
              <div className="h-8 bg-gray-200 rounded w-3/4" />
              <div className="h-4 bg-gray-200 rounded" />
              <div className="h-4 bg-gray-200 rounded w-5/6" />
              <div className="h-12 bg-gray-200 rounded w-1/3" />
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!product) {
    return (
      <div className="max-w-6xl mx-auto">
        <div className="card p-12 text-center">
          <p className="text-gray-500">Product not found</p>
          <button
            onClick={() => navigate("/products")}
            className="btn-primary mt-4"
          >
            Back to Products
          </button>
        </div>
      </div>
    );
  }

  const images = [
    product.imageUrl,
    `https://picsum.photos/600/400?random=${product.productId}_2`,
    `https://picsum.photos/600/400?random=${product.productId}_3`,
    `https://picsum.photos/600/400?random=${product.productId}_4`,
  ];

  return (
    <div className="max-w-6xl mx-auto">
      <div className="card p-8">
        <div className="grid grid-cols-2 gap-8">
          {/* Image Gallery */}
          <div>
            <div className="relative h-96 rounded-lg overflow-hidden bg-gradient-to-br from-gray-100 to-gray-200 mb-4">
              <img
                src={images[selectedImage]}
                alt={product.name}
                className="w-full h-full object-cover"
              />
              {product.inventory < 10 && product.inventory > 0 && (
                <div className="absolute top-4 left-4 badge bg-yellow-100 text-yellow-800">
                  Only {product.inventory} left in stock!
                </div>
              )}
            </div>
            <div className="grid grid-cols-4 gap-2">
              {images.map((img, index) => (
                <button
                  key={index}
                  onClick={() => setSelectedImage(index)}
                  className={`relative h-20 rounded-lg overflow-hidden ${
                    selectedImage === index ? "ring-2 ring-primary-500" : ""
                  }`}
                >
                  <img
                    src={img}
                    alt=""
                    className="w-full h-full object-cover"
                  />
                </button>
              ))}
            </div>
          </div>

          {/* Product Info */}
          <div>
            <div className="mb-4">
              <span className="badge bg-secondary-100 text-secondary-800">
                {product.category}
              </span>
              {product.tags?.map((tag: string) => (
                <span
                  key={tag}
                  className="ml-2 badge bg-gray-100 text-gray-700"
                >
                  {tag}
                </span>
              ))}
            </div>

            <h1 className="text-3xl font-bold text-gray-900 mb-4">
              {product.name}
            </h1>

            <div className="flex items-center mb-4">
              <div className="flex items-center">
                {[...Array(5)].map((_, i) => (
                  <svg
                    key={i}
                    className={`w-5 h-5 ${
                      i < Math.floor(product.rating)
                        ? "text-yellow-400"
                        : "text-gray-300"
                    }`}
                    fill="currentColor"
                    viewBox="0 0 20 20"
                  >
                    <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                  </svg>
                ))}
                <span className="ml-2 text-gray-600">
                  {product.rating} ({product.reviewCount} reviews)
                </span>
              </div>
            </div>

            <p className="text-gray-600 mb-6">{product.description}</p>

            <div className="border-t border-b py-4 mb-6">
              <div className="flex items-center justify-between mb-4">
                <span className="text-3xl font-bold text-gray-900">
                  ${product.price.toFixed(2)}
                </span>
                <span
                  className={`text-sm ${
                    product.inventory > 0 ? "text-green-600" : "text-red-600"
                  }`}
                >
                  {product.inventory > 0 ? "✓ In Stock" : "✗ Out of Stock"}
                </span>
              </div>

              <div className="flex items-center space-x-4">
                <div className="flex items-center">
                  <button
                    onClick={() => setQuantity(Math.max(1, quantity - 1))}
                    className="w-10 h-10 rounded-lg bg-gray-200 hover:bg-gray-300 flex items-center justify-center"
                  >
                    -
                  </button>
                  <input
                    type="number"
                    value={quantity}
                    onChange={(e) => {
                      const newQuantity = Math.max(
                        1,
                        parseInt(e.target.value) || 1
                      );
                      const stock = product?.inventory ?? 0;
                      setQuantity(Math.min(newQuantity, stock));
                    }}
                    className="w-16 h-10 text-center border-t border-b"
                  />
                  <button
                    onClick={() => {
                      const stock = product?.inventory ?? 0;
                      setQuantity((prev) => Math.min(prev + 1, stock));
                    }}
                    disabled={quantity >= (product?.inventory ?? 0)}
                    className="w-10 h-10 rounded-lg bg-gray-200 hover:bg-gray-300 flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    +
                  </button>
                </div>

                <button
                  onClick={handleAddToCart}
                  disabled={product.inventory === 0}
                  className={`flex-1 ${
                    product.inventory === 0
                      ? "bg-gray-300 cursor-not-allowed"
                      : "btn-primary"
                  }`}
                >
                  {product.inventory === 0 ? "Out of Stock" : "Add to Cart"}
                </button>
              </div>
            </div>

            {/* Additional Info */}
            <div className="space-y-2 text-sm text-gray-600">
              <p>✓ Free shipping on orders over $50</p>
              <p>✓ 30-day return policy</p>
              <p>✓ Secure checkout</p>
              <p>✓ Real-time inventory tracking powered by Apache Flink</p>
            </div>
          </div>
        </div>

        {/* Additional Product Details Section */}
        <div className="grid grid-cols-3 gap-8 mt-8">
          {/* Product Specifications */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold mb-4">Specifications</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-gray-600">SKU:</dt>
                <dd className="font-medium">KS-{product.productId}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Category:</dt>
                <dd className="font-medium">{product.category}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Stock:</dt>
                <dd className="font-medium">{product.inventory} units</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Weight:</dt>
                <dd className="font-medium">
                  {(Math.random() * 5 + 0.5).toFixed(1)} lbs
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Dimensions:</dt>
                <dd className="font-medium">12" x 8" x 4"</dd>
              </div>
            </dl>
          </div>

          {/* Features */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold mb-4">Key Features</h3>
            <ul className="space-y-2 text-gray-600">
              <li className="flex items-start">
                <svg
                  className="w-5 h-5 text-green-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                    clipRule="evenodd"
                  />
                </svg>
                <span>Premium quality materials</span>
              </li>
              <li className="flex items-start">
                <svg
                  className="w-5 h-5 text-green-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                    clipRule="evenodd"
                  />
                </svg>
                <span>Eco-friendly packaging</span>
              </li>
              <li className="flex items-start">
                <svg
                  className="w-5 h-5 text-green-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                    clipRule="evenodd"
                  />
                </svg>
                <span>1-year warranty included</span>
              </li>
              <li className="flex items-start">
                <svg
                  className="w-5 h-5 text-green-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                    clipRule="evenodd"
                  />
                </svg>
                <span>Expert customer support</span>
              </li>
            </ul>
          </div>

          {/* Shipping Info */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold mb-4">Shipping Information</h3>
            <div className="space-y-3">
              <div className="flex items-start">
                <svg
                  className="w-5 h-5 text-blue-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"
                  />
                </svg>
                <div>
                  <p className="font-medium">Standard Delivery</p>
                  <p className="text-sm text-gray-600">5-7 business days</p>
                </div>
              </div>
              <div className="flex items-start">
                <svg
                  className="w-5 h-5 text-blue-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M13 10V3L4 14h7v7l9-11h-7z"
                  />
                </svg>
                <div>
                  <p className="font-medium">Express Delivery</p>
                  <p className="text-sm text-gray-600">2-3 business days</p>
                </div>
              </div>
              <div className="flex items-start">
                <svg
                  className="w-5 h-5 text-blue-500 mr-2 flex-shrink-0 mt-0.5"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
                <div>
                  <p className="font-medium">Track Your Order</p>
                  <p className="text-sm text-gray-600">Real-time updates</p>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Customer Reviews Section */}
        <div className="mt-8 pt-8 border-t">
          <h3 className="text-xl font-semibold mb-6">Customer Reviews</h3>
          <div className="grid grid-cols-2 gap-6">
            {/* Review Summary */}
            <div className="card p-6">
              <h4 className="text-lg font-semibold mb-4">Rating Summary</h4>
              <div className="flex items-center mb-4">
                <span className="text-3xl font-bold">{product.rating}</span>
                <div className="ml-3">
                  <div className="flex items-center">
                    {[...Array(5)].map((_, i) => (
                      <svg
                        key={i}
                        className={`w-5 h-5 ${
                          i < Math.floor(product.rating)
                            ? "text-yellow-400"
                            : "text-gray-300"
                        }`}
                        fill="currentColor"
                        viewBox="0 0 20 20"
                      >
                        <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                      </svg>
                    ))}
                  </div>
                  <p className="text-sm text-gray-600">
                    Based on {product.reviewCount} reviews
                  </p>
                </div>
              </div>
              {/* Rating bars */}
              {[5, 4, 3, 2, 1].map((stars) => (
                <div key={stars} className="flex items-center mb-1">
                  <span className="text-sm w-8">{stars}★</span>
                  <div className="flex-1 mx-2 bg-gray-200 rounded-full h-2">
                    <div
                      className="bg-yellow-400 h-2 rounded-full"
                      style={{
                        width: `${
                          stars === 5
                            ? 60
                            : stars === 4
                            ? 25
                            : stars === 3
                            ? 10
                            : 5
                        }%`,
                      }}
                    />
                  </div>
                  <span className="text-sm text-gray-600 w-10">
                    {stars === 5 ? 60 : stars === 4 ? 25 : stars === 3 ? 10 : 5}
                    %
                  </span>
                </div>
              ))}
            </div>

            {/* Recent Reviews */}
            <div className="space-y-4">
              <h4 className="text-lg font-semibold">Recent Reviews</h4>
              {[1, 2, 3].map((review) => (
                <div key={review} className="border-b pb-4">
                  <div className="flex items-center mb-2">
                    <div className="flex items-center">
                      {[...Array(5)].map((_, i) => (
                        <svg
                          key={i}
                          className={`w-4 h-4 ${
                            i < 4 ? "text-yellow-400" : "text-gray-300"
                          }`}
                          fill="currentColor"
                          viewBox="0 0 20 20"
                        >
                          <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                        </svg>
                      ))}
                    </div>
                    <span className="ml-2 text-sm font-medium">
                      Customer {review}
                    </span>
                    <span className="ml-auto text-sm text-gray-500">
                      {review} days ago
                    </span>
                  </div>
                  <p className="text-gray-700">
                    Great product! Exactly as described and arrived quickly. The
                    real-time inventory tracking gave me confidence in my
                    purchase.
                  </p>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Powered by Tech Stack */}
        <div className="mt-8 pt-8 border-t text-center">
          <p className="text-sm text-gray-500">
            Powered by Apache Flink & Apache Paimon for real-time inventory
            management
          </p>
        </div>
      </div>
    </div>
  );
};

export default ProductDetailPage;
