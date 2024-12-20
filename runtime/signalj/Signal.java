/* Copyright (c) 2016-2017, Tetsuo Kamina
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package signalj;

import io.reactivex.Flowable;
import io.reactivex.processors.BehaviorProcessor;
import io.reactivex.functions.Function;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;

public class Signal<T> {
    protected T inner;
    private BehaviorProcessor<T> processor;
    protected T initial = null;
    protected T last = null;
    protected Flowable<T> or = null;
    protected T sum = null;
    protected int count = 0;

    public Signal() {}

    public Signal(T value) {
	initial = value;
	inner = value;
	processor = BehaviorProcessor.create();
	processor.onNext(value);

	//harano
	if (value instanceof ObjectSignal) {
	    ((ObjectSignal)value).setSignal(this);
	}
    }

    protected void setInnerValue(T value) {
	last = inner;
	inner = value;
	count++;
	if (sum == null) sum = value;
	else sum = computeSumInner(sum, value);
    }

    public void setEffective() {
	processor.subscribe(this::setInnerValue);
    }

    protected T computeSumInner(T sum, T value) {
	if (sum instanceof Integer) {
	    Integer num = (Integer)sum;
	    num += (Integer)value;
	    sum = (T)num;
	} else if (sum instanceof Long) {
	    Long num = (Long)sum;
	    num += (Long)value;
	    sum = (T)num;
	} else if (sum instanceof Float) {
	    Float num = (Float)sum;
	    num += (Float)value;
	    sum = (T)num;
	} else if (sum instanceof Double) {
	    Double num = (Double)sum;
	    num += (Double)value;
	    sum = (T)num;
	}
	return sum;
    }

    public void set(T value) {
	processor.onNext(value);
    }
    
    public T __signalj__get() {
	return processor.blockingFirst();
    }

    public void subscribe(Consumer<T> cs) {
	processor.subscribe(cs);
    }

    public T last(T def) {
	if (last == null) return def;
	else return last;
    }

    public T sum() {
	return sum;
    }

    public T value() { return processor.blockingFirst(); }

    public int count() { return count; }
    
    public T or(T arg, T source) {
	if (value() == source) return source;
	else return arg;
    }

    public T when(Predicate<T> pred, T def) {
	boolean p = false;
	try { p = pred.test(value()); } catch (Exception e) { }
	if (p) return value();
	else return def;
    }

    public T fold(Function<T,T> f) {
	try {
	    inner = f.apply(inner);
	} catch (Exception e) { }
	return inner;
    }

    public void reset() {
	inner = initial;
	count = 0;
    }

    public Flowable<T> getFlowable() {
	return processor;
    }
}
